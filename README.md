# pbcc

pbcc is an implementation of Protocol Buffers for Python, designed for speed, safety, and ease of use. It supports serializing and deserializing messages in standard Protobuf format, which is compatible with other Protobuf libraries. pbcc does not support gRPC service definitions; it only supports message structures.

Here is a contrived example Protobuf definition:

```
// my_interface.proto
syntax = "proto3";
enum MyEnum {
    VALUE0 = 0;
    VALUE3 = 3;
}
message LongMessage {
    oneof f_oneof {
        MyEnum f_enum = 1;
        string f_string = 2;
    }
    repeated uint64 f_uint64 = 3;
    optional bytes f_maybe_bytes = 4;
    map<string, float> f_map_str_float = 5;
}
```

To build a file like this, run `uv run compile.py --proto-files my_interface.proto --output-basename my_interface`. This will produce the files my_interface.cc (the C++ extension module source), my_interface.so (the compiled C++ extension module), and my_interface.pyi (the type annotations for the extension module). Here is the resulting pbcc module's interface:

```python
# Since multiple .proto modules can be built into a single pbcc module, the
# classes within that module are namespaced according to the .proto filenames
class my_interface:

    class MyEnum(IntEnum):
        VALUE0 = 0
        VALUE3 = 3

    class LongMessage:
        f_oneof: my_interface.MyEnum | str
        f_uint64: list[int]
        f_maybe_bytes: bytes | None
        f_map_str_float: dict[str, float]

        # Constructs a new LongMessage
        def __init__(self, *,
            f_oneof: MyEnum = MyEnum.VALUE0,
            f_uint64: list[int] = 0,
            f_maybe_bytes: bytes | None = None,
            f_map_str_float: dict[str, float] = {},
        ): ...

        # Parses a byte string into a new LongMessage
        @staticmethod
        def from_proto_data(
            data: bytes,
            retain_unknown_fields: bool = True,
            ignore_incorrect_types: bool = False,
        ) -> LongMessage: ...

        # Parses a byte string into an existing LongMessage object
        def parse_proto_into_this(
            self,
            data: bytes,
            retain_unknown_fields: bool = True,
            ignore_incorrect_types: bool = False,
        ) -> None: ...

        # Serializes an existing LongMessage object into a byte string
        def as_proto_data(self) -> bytes: ...

        # Returns a dict with the same fields as this object
        def as_dict(self) -> dict[str, Any]: ...

        # Makes a copy of this object, with some fields optionally replaced
        def proto_copy(self, *,
            f_oneof: MyEnum | str = ...,
            f_uint64: list[int] = ...,
            f_maybe_bytes: bytes | None = ...,
            f_map_str_float: dict[str, float] = ...,
        ) -> LongMessage: ...

        # Functions for dealing with unparsed fields that weren't part of the message definition
        def has_unknown_fields(self) -> bool: ...
        def delete_unknown_fields(self) -> None: ...
        def get_unknown_fields(self) -> dict[int, bytes]: ...
```
