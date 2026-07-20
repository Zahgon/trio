
from __future__ import annotations

import contextlib
import enum
import errno
import hmac
import os
import struct
import warnings
import weakref
from itertools import count
from typing import TYPE_CHECKING, Generic, TypeAlias, TypeVar
from weakref import ReferenceType, WeakValueDictionary

import attrs

import trio

from ._util import NoPublicConstructor, final

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Iterator
    from types import TracebackType

    from OpenSSL import SSL  # noqa: TC004
    from typing_extensions import Self, TypeVarTuple, Unpack

    from trio._socket import AddressFormat
    from trio.socket import SocketType

    PosArgsT = TypeVarTuple("PosArgsT")

MAX_UDP_PACKET_SIZE = 65527


def packet_header_overhead(sock: SocketType) -> int:
    if sock.family == trio.socket.AF_INET:
        return 28
    else:
        return 48


def worst_case_mtu(sock: SocketType) -> int:
    if sock.family == trio.socket.AF_INET:
        return 576 - packet_header_overhead(sock)
    else:
        return 1280 - packet_header_overhead(sock)  # TODO: test this line




class ContentType(enum.IntEnum):
    change_cipher_spec = 20
    alert = 21
    handshake = 22
    application_data = 23
    heartbeat = 24


class HandshakeType(enum.IntEnum):
    hello_request = 0
    client_hello = 1
    server_hello = 2
    hello_verify_request = 3
    new_session_ticket = 4
    end_of_early_data = 4
    encrypted_extensions = 8
    certificate = 11
    server_key_exchange = 12
    certificate_request = 13
    server_hello_done = 14
    certificate_verify = 15
    client_key_exchange = 16
    finished = 20
    certificate_url = 21
    certificate_status = 22
    supplemental_data = 23
    key_update = 24
    compressed_certificate = 25
    ekt_key = 26
    message_hash = 254


class ProtocolVersion:
    DTLS10 = bytes([254, 255])
    DTLS12 = bytes([254, 253])


EPOCH_MASK = 0xFFFF << (6 * 8)


class BadPacket(Exception):
    pass






RECORD_HEADER = struct.Struct("!B2sQH")




@attrs.frozen
class Record:
    content_type: int
    version: bytes = attrs.field(repr=to_hex)
    epoch_seqno: int
    payload: bytes = attrs.field(repr=to_hex)


def records_untrusted(packet: bytes) -> Iterator[Record]:
    i = 0
    while i < len(packet):
        try:
            ct, version, epoch_seqno, payload_len = RECORD_HEADER.unpack_from(packet, i)
        except struct.error as exc:  # pragma: no cover
            raise BadPacket("invalid record header") from exc
        i += RECORD_HEADER.size
        payload = packet[i : i + payload_len]
        if len(payload) != payload_len:
            raise BadPacket("short record")
        i += payload_len
        yield Record(ct, version, epoch_seqno, payload)


def encode_record(record: Record) -> bytes:
    header = RECORD_HEADER.pack(
        record.content_type,
        record.version,
        record.epoch_seqno,
        len(record.payload),
    )
    return header + record.payload


HANDSHAKE_MESSAGE_HEADER = struct.Struct("!B3sH3s3s")


@attrs.frozen
class HandshakeFragment:
    msg_type: int
    msg_len: int
    msg_seq: int
    frag_offset: int
    frag_len: int
    frag: bytes = attrs.field(repr=to_hex)


def decode_handshake_fragment_untrusted(payload: bytes) -> HandshakeFragment:
    # Raises BadPacket if decoding fails
    try:
        (
            msg_type,
            msg_len_bytes,
            msg_seq,
            frag_offset_bytes,
            frag_len_bytes,
        ) = HANDSHAKE_MESSAGE_HEADER.unpack_from(payload)
    except struct.error as exc:  # TODO: test this line
        raise BadPacket("bad handshake message header") from exc
    msg_len = int.from_bytes(msg_len_bytes, "big")
    frag_offset = int.from_bytes(frag_offset_bytes, "big")
    frag_len = int.from_bytes(frag_len_bytes, "big")
    frag = payload[HANDSHAKE_MESSAGE_HEADER.size :]
    if len(frag) != frag_len:
        raise BadPacket("handshake fragment length doesn't match record length")
    return HandshakeFragment(
        msg_type,
        msg_len,
        msg_seq,
        frag_offset,
        frag_len,
        frag,
    )


def encode_handshake_fragment(hsf: HandshakeFragment) -> bytes:
    pass


def decode_client_hello_untrusted(packet: bytes) -> tuple[int, bytes, bytes]:
    try:
        record = next(records_untrusted(packet))
        if record.content_type != ContentType.handshake:  # pragma: no cover
            raise BadPacket("not a handshake record")
        fragment = decode_handshake_fragment_untrusted(record.payload)
        if fragment.msg_type != HandshakeType.client_hello:
            raise BadPacket("not a ClientHello")
        if fragment.frag_offset != 0:
            raise BadPacket("fragmented ClientHello")
        if fragment.frag_len != fragment.msg_len:
            raise BadPacket("fragmented ClientHello")


        body = fragment.frag
        session_id_len = body[2 + 32]
        cookie_len_offset = 2 + 32 + 1 + session_id_len
        cookie_len = body[cookie_len_offset]

        cookie_start = cookie_len_offset + 1
        cookie_end = cookie_start + cookie_len

        before_cookie = body[:cookie_len_offset]
        cookie = body[cookie_start:cookie_end]
        after_cookie = body[cookie_end:]

        if len(cookie) != cookie_len:
            raise BadPacket("short cookie")
        return (record.epoch_seqno, cookie, before_cookie + after_cookie)

    except (struct.error, IndexError) as exc:
        raise BadPacket("bad ClientHello") from exc


@attrs.frozen
class HandshakeMessage:
    record_version: bytes = attrs.field(repr=to_hex)
    msg_type: HandshakeType
    msg_seq: int
    body: bytearray = attrs.field(repr=to_hex)


@attrs.frozen
class PseudoHandshakeMessage:
    record_version: bytes = attrs.field(repr=to_hex)
    content_type: int
    payload: bytes = attrs.field(repr=to_hex)


@attrs.frozen
class OpaqueHandshakeMessage:
    record: Record


_AnyHandshakeMessage: TypeAlias = (
    HandshakeMessage | PseudoHandshakeMessage | OpaqueHandshakeMessage
)


def decode_volley_trusted(
    volley: bytes,
) -> list[_AnyHandshakeMessage]:
    messages: list[_AnyHandshakeMessage] = []
    messages_by_seq = {}
    for record in records_untrusted(volley):
        if record.epoch_seqno & EPOCH_MASK:
            messages.append(OpaqueHandshakeMessage(record))
        elif record.content_type in (ContentType.change_cipher_spec, ContentType.alert):
            messages.append(
                PseudoHandshakeMessage(
                    record.version,
                    record.content_type,
                    record.payload,
                ),
            )
        else:
            assert record.content_type == ContentType.handshake
            fragment = decode_handshake_fragment_untrusted(record.payload)
            msg_type = HandshakeType(fragment.msg_type)
            if fragment.msg_seq not in messages_by_seq:
                msg = HandshakeMessage(
                    record.version,
                    msg_type,
                    fragment.msg_seq,
                    bytearray(fragment.msg_len),
                )
                messages.append(msg)
                messages_by_seq[fragment.msg_seq] = msg
            else:
                msg = messages_by_seq[fragment.msg_seq]
            assert msg.msg_type == fragment.msg_type
            assert msg.msg_seq == fragment.msg_seq
            assert len(msg.body) == fragment.msg_len

            msg.body[
                fragment.frag_offset : fragment.frag_offset + fragment.frag_len
            ] = fragment.frag

    return messages


class RecordEncoder:
    def __init__(self) -> None:
        self._record_seq = count()


    def encode_volley(
        self,
        messages: Iterable[_AnyHandshakeMessage],
        mtu: int,
    ) -> list[bytearray]:
        packets = []
        packet = bytearray()
        for message in messages:
            if isinstance(message, OpaqueHandshakeMessage):
                encoded = encode_record(message.record)
                if mtu - len(packet) - len(encoded) <= 0:  # TODO: test this line
                    packets.append(packet)
                    packet = bytearray()
                packet += encoded
                assert len(packet) <= mtu
            elif isinstance(message, PseudoHandshakeMessage):
                space = mtu - len(packet) - RECORD_HEADER.size - len(message.payload)
                if space <= 0:  # TODO: test this line
                    packets.append(packet)
                    packet = bytearray()
                packet += RECORD_HEADER.pack(
                    message.content_type,
                    message.record_version,
                    next(self._record_seq),
                    len(message.payload),
                )
                packet += message.payload
                assert len(packet) <= mtu
            else:
                msg_len_bytes = len(message.body).to_bytes(3, "big")
                frag_offset = 0
                frags_encoded = 0
                while frag_offset < len(message.body) or not frags_encoded:
                    space = (
                        mtu
                        - len(packet)
                        - RECORD_HEADER.size
                        - HANDSHAKE_MESSAGE_HEADER.size
                    )
                    if space <= 0:
                        packets.append(packet)
                        packet = bytearray()
                        continue
                    frag = message.body[frag_offset : frag_offset + space]
                    frag_offset_bytes = frag_offset.to_bytes(3, "big")
                    frag_len_bytes = len(frag).to_bytes(3, "big")
                    frag_offset += len(frag)

                    packet += RECORD_HEADER.pack(
                        ContentType.handshake,
                        message.record_version,
                        next(self._record_seq),
                        HANDSHAKE_MESSAGE_HEADER.size + len(frag),
                    )

                    packet += HANDSHAKE_MESSAGE_HEADER.pack(
                        message.msg_type,
                        msg_len_bytes,
                        message.msg_seq,
                        frag_offset_bytes,
                        frag_len_bytes,
                    )

                    packet += frag

                    frags_encoded += 1
                    assert len(packet) <= mtu

        if packet:
            packets.append(packet)

        return packets



COOKIE_REFRESH_INTERVAL = 30  # seconds
KEY_BYTES = 32
COOKIE_HASH = "sha256"
SALT_BYTES = 8
COOKIE_LENGTH = 32












_T = TypeVar("_T")


class _Queue(Generic[_T]):
    def __init__(self, incoming_packets_buffer: int | float) -> None:  # noqa: PYI041
        self.s, self.r = trio.open_memory_channel[_T](incoming_packets_buffer)


def _read_loop(read_fn: Callable[[int], bytes]) -> bytes:
    chunks = []
    while True:
        try:
            chunk = read_fn(2**14)  # max TLS record size
        except SSL.WantReadError:
            break
        chunks.append(chunk)
    return b"".join(chunks)






@attrs.frozen
class DTLSChannelStatistics:

    incoming_packets_dropped_in_trio: int


@final
class DTLSChannel(trio.abc.Channel[bytes], metaclass=NoPublicConstructor):

    def __init__(
        self,
        endpoint: DTLSEndpoint,
        peer_address: AddressFormat,
        ctx: SSL.Context,
    ) -> None:
        self.endpoint = endpoint
        self.peer_address = peer_address
        self._packets_dropped_in_trio = 0
        self._client_hello = None
        self._did_handshake = False
        self._ssl = SSL.Connection(ctx)
        self._handshake_mtu = 0
        self.set_ciphertext_mtu(best_guess_mtu(self.endpoint.socket))
        self._replaced = False
        self._closed = False
        self._q = _Queue[bytes](endpoint.incoming_packets_buffer)
        self._handshake_lock = trio.Lock()
        self._record_encoder: RecordEncoder = RecordEncoder()

        self._final_volley: list[_AnyHandshakeMessage] = []

    def _set_replaced(self) -> None:
        self._replaced = True
        self._q.s.close()

    def _check_replaced(self) -> None:
        if self._replaced:
            raise trio.BrokenResourceError(
                "peer tore down this connection to start a new one",
            )



    def close(self) -> None:
        """Close this connection.

        `DTLSChannel`\\s don't actually own any OS-level resources – the
        socket is owned by the `DTLSEndpoint`, not the individual connections. So
        you don't really *have* to call this. But it will interrupt any other tasks
        calling `receive` with a `ClosedResourceError`, and cause future attempts to use
        this connection to fail.

        You can also use this object as a synchronous or asynchronous context manager.

        """
        if self._closed:
            return
        self._closed = True
        if self.endpoint._streams.get(self.peer_address) is self:
            del self.endpoint._streams[self.peer_address]
        self._q.r.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return self.close()

    async def aclose(self) -> None:
        """Close this connection, but asynchronously.

        This is included to satisfy the `trio.abc.Channel` contract. It's
        identical to `close`, but async.

        """
        self.close()
        await trio.lowlevel.checkpoint()

    async def _send_volley(self, volley_messages: list[_AnyHandshakeMessage]) -> None:
        packets = self._record_encoder.encode_volley(
            volley_messages,
            self._handshake_mtu,
        )
        for packet in packets:
            async with self.endpoint._send_lock:
                await self.endpoint.socket.sendto(packet, self.peer_address)


    async def do_handshake(self, *, initial_retransmit_timeout: float = 1.0) -> None:
        """Perform the handshake.

        Calling this is optional – if you don't, then it will be automatically called
        the first time you call `send` or `receive`. But calling it explicitly can be
        useful in case you want to control the retransmit timeout, use a cancel scope to
        place an overall timeout on the handshake, or catch errors from the handshake
        specifically.

        It's safe to call this multiple times, or call it simultaneously from multiple
        tasks – the first call will perform the handshake, and the rest will be no-ops.

        Args:

          initial_retransmit_timeout (float): Since UDP is an unreliable protocol, it's
            possible that some of the packets we send during the handshake will get
            lost. To handle this, DTLS uses a timer to automatically retransmit
            handshake packets that don't receive a response. This lets you set the
            timeout we use to detect packet loss. Ideally, it should be set to ~1.5
            times the round-trip time to your peer, but 1 second is a reasonable
            default. There's `some useful guidance here
            <https://tlswg.org/dtls13-spec/draft-ietf-tls-dtls13.html#name-timer-values>`__.

            This is the *initial* timeout, because if packets keep being lost then Trio
            will automatically back off to longer values, to avoid overloading the
            network.

        """
        async with self._handshake_lock:
            if self._did_handshake:
                return

            timeout = initial_retransmit_timeout
            volley_messages: list[_AnyHandshakeMessage] = []
            volley_failed_sends = 0

            def read_volley() -> list[_AnyHandshakeMessage]:
                volley_bytes = _read_loop(self._ssl.bio_read)
                new_volley_messages = decode_volley_trusted(volley_bytes)
                if (
                    new_volley_messages
                    and volley_messages
                    and isinstance(new_volley_messages[0], HandshakeMessage)
                    and isinstance(volley_messages[0], HandshakeMessage)
                    and new_volley_messages[0].msg_seq == volley_messages[0].msg_seq
                ):
                    return []
                else:
                    return new_volley_messages

            with contextlib.suppress(SSL.WantReadError):
                self._ssl.do_handshake()
            volley_messages = read_volley()
            if not volley_messages:  # pragma: no cover
                raise SSL.Error("something wrong with peer's ClientHello")

            while True:
                assert volley_messages
                self._check_replaced()
                await self._send_volley(volley_messages)
                self.endpoint._ensure_receive_loop()
                with trio.move_on_after(timeout) as cscope:
                    async for packet in self._q.r:
                        self._ssl.bio_write(packet)
                        try:
                            self._ssl.do_handshake()
                        except (SSL.WantReadError, SSL.Error):
                            pass
                        else:
                            self._did_handshake = True
                            self._final_volley = read_volley()
                            await self._send_volley(self._final_volley)
                            return
                        maybe_volley = read_volley()
                        if maybe_volley:
                            if (
                                isinstance(maybe_volley[0], PseudoHandshakeMessage)
                                and maybe_volley[0].content_type == ContentType.alert
                            ):  # TODO: test this line
                                await self._send_volley(maybe_volley)
                            else:
                                volley_messages = maybe_volley
                                if volley_failed_sends == 0:
                                    timeout = initial_retransmit_timeout
                                volley_failed_sends = 0
                                break
                    else:
                        assert self._replaced
                        self._check_replaced()
                if cscope.cancelled_caught:
                    timeout = min(2 * timeout, 60.0)
                    volley_failed_sends += 1
                    if volley_failed_sends == 2:
                        self._handshake_mtu = min(
                            self._handshake_mtu,
                            worst_case_mtu(self.endpoint.socket),
                        )

    async def send(self, data: bytes) -> None:
        """Send a packet of data, securely."""

        if self._closed:
            raise trio.ClosedResourceError
        if not data:
            raise ValueError("openssl doesn't support sending empty DTLS packets")
        if not self._did_handshake:
            await self.do_handshake()
        self._check_replaced()
        self._ssl.write(data)
        async with self.endpoint._send_lock:
            await self.endpoint.socket.sendto(
                _read_loop(self._ssl.bio_read),
                self.peer_address,
            )

    async def receive(self) -> bytes:
        """Fetch the next packet of data from this connection's peer, waiting if
        necessary.

        This is safe to call from multiple tasks simultaneously, in case you have some
        reason to do that. And more importantly, it's cancellation-safe, meaning that
        cancelling a call to `receive` will never cause a packet to be lost or corrupt
        the underlying connection.

        """
        if not self._did_handshake:
            await self.do_handshake()
        while True:
            try:
                packet = await self._q.r.receive()
            except trio.EndOfChannel:
                assert self._replaced
                self._check_replaced()
            self._ssl.bio_write(packet)
            cleartext = _read_loop(self._ssl.read)
            if cleartext:
                return cleartext

    def set_ciphertext_mtu(self, new_mtu: int) -> None:
        pass

    def get_cleartext_mtu(self) -> int:
        pass

    def statistics(self) -> DTLSChannelStatistics:
        """Returns a `DTLSChannelStatistics` object with statistics about this connection."""
        return DTLSChannelStatistics(self._packets_dropped_in_trio)


@final
class DTLSEndpoint:

    def __init__(
        self,
        socket: SocketType,
        *,
        incoming_packets_buffer: int = 10,
    ) -> None:
        global SSL
        from OpenSSL import SSL

        self._initialized: bool = False
        if socket.type != trio.socket.SOCK_DGRAM:
            raise ValueError("DTLS requires a SOCK_DGRAM socket")
        self._initialized = True
        self.socket: SocketType = socket

        self.incoming_packets_buffer = incoming_packets_buffer
        self._token = trio.lowlevel.current_trio_token()
        self._streams: WeakValueDictionary[AddressFormat, DTLSChannel] = (
            WeakValueDictionary()
        )
        self._listening_context: SSL.Context | None = None
        self._listening_key: bytes | None = None
        self._incoming_connections_q = _Queue[DTLSChannel](float("inf"))
        self._send_lock = trio.Lock()
        self._closed = False
        self._receive_loop_spawned = False

    def _ensure_receive_loop(self) -> None:
        if not self._receive_loop_spawned:
            trio.lowlevel.spawn_system_task(
                dtls_receive_loop,
                weakref.ref(self),
                self.socket,
            )
            self._receive_loop_spawned = True

    def __del__(self) -> None:
        if not self._initialized:
            return
        if not self._closed:
            with contextlib.suppress(RuntimeError):
                self._token.run_sync_soon(self.close)
            warnings.warn(
                f"unclosed DTLS endpoint {self!r}",
                ResourceWarning,
                source=self,
                stacklevel=1,
            )

    def close(self) -> None:
        """Close this socket, and all associated DTLS connections.

        This object can also be used as a context manager.

        """
        self._closed = True
        self.socket.close()
        for stream in list(self._streams.values()):
            stream.close()
        self._incoming_connections_q.s.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return self.close()

    def _check_closed(self) -> None:
        if self._closed:
            raise trio.ClosedResourceError

    async def serve(
        self,
        ssl_context: SSL.Context,
        async_fn: Callable[[DTLSChannel, Unpack[PosArgsT]], Awaitable[object]],
        *args: Unpack[PosArgsT],
        task_status: trio.TaskStatus[None] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        pass

    def connect(
        self,
        address: tuple[str, int],
        ssl_context: SSL.Context,
    ) -> DTLSChannel:
        """Initiate an outgoing DTLS connection.

        Notice that this is a synchronous method. That's because it doesn't actually
        initiate any I/O – it just sets up a `DTLSChannel` object. The actual handshake
        doesn't occur until you start using the `DTLSChannel`. This gives you a chance
        to do further configuration first, like setting MTU etc.

        Args:
          address: The address to connect to. Usually a (host, port) tuple, like
            ``("127.0.0.1", 12345)``.
          ssl_context (OpenSSL.SSL.Context): The PyOpenSSL context object to use for
            this connection.

        Returns:
          DTLSChannel

        """
        self._check_closed()
        set_ssl_context_options(ssl_context)
        channel = DTLSChannel._create(self, address, ssl_context)
        channel._ssl.set_connect_state()
        old_channel = self._streams.get(address)
        if old_channel is not None:
            old_channel._set_replaced()
        self._streams[address] = channel
        return channel


def set_ssl_context_options(ctx: SSL.Context) -> None:
    ctx.set_options(
        SSL.OP_NO_QUERY_MTU | SSL.OP_NO_RENEGOTIATION,  # type: ignore[attr-defined]
    )
