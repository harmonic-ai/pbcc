"""Test module for pbcc.

This is NOT a pytest or unittest file; it should instead be run directly via `uv run test.py`. This is because it
compiles pb2 and pbcc modules at import time, so it can then import them.

"""

import os
import pickle
import subprocess
import sys
import traceback
from types import FunctionType
from typing import Any, ClassVar, Protocol, Sequence, cast

from google.protobuf.message import Message

print("Building test_pb2")
os.makedirs("test_modules", exist_ok=True)
subprocess.check_call(
    (
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I.",
        "test.proto",
        "--python_out=test_modules",
        "--pyi_out=test_modules",
    )
)
import test_modules.test_pb2 as pb  # noqa: E402

print("Building test_pbcc")
subprocess.check_call(
    (sys.executable, "compile.py", "test_modules.test_pb2", "--output-basename", "test_modules/test_pbcc")
)
import test_modules.test_pbcc as pbcc  # noqa: E402


class PBCCMessage(Protocol):
    @staticmethod
    def from_proto_data(data: bytes, retain_unknown_fields: bool = True) -> Any: ...
    def as_proto_data(self) -> bytes: ...
    def proto_copy(*args, **kwargs) -> Any: ...
    def as_dict(self) -> dict[str, Any]: ...
    def has_unknown_fields(self) -> bool: ...
    def delete_unknown_fields(self) -> None: ...


# Things to test:
# + For all possible types (including optional/required/repeated and maps):
#   + Default construction (no args to __init__)
#   + Deserializing a blank message
#   + Passing args to __init__
#   + Reading values from attributes after any of the above three
#   + Assigning a value to an attribute and reading it again
#   + Serializing and deserializing
#     + Unknown fields should be passed through (declare a message type with a subset of another message's fields to test this)
#     + Unknown fields should be destroyed if retain_unknown_fields is False
#   + Specific values
#     + Values with the wrong types for their field (e.g. a list where an int is expected, etc)
#     + Floats and ints should be interchangeable (mostly)
#     + Unsigned int: 0, 1, max, negative, out of range
#     + Signed int: 0, 1, -1, min, max, out of range on either side
#     + Fixed: same as above
#     + Float/double: 0, 1, -1, min, max, inf, -inf, nan
#     + Bytes: empty, short, long
#     + Str: empty, ascii, unicode
#     + Repeated: all of the above
#     + Optional: all of the above
#     + Maps (value types all of the above, key types primitive only)
#     + Submessages
#   + str() and repr() do reasonable things
# - Nested message declarations
# + Deserializing invalid data (is StringReader out_of_range handled properly?)
# + Ensure all of the above is cross-compatible with the Google libs (serialize with pbcc and deserialize with Google, and vice versa)
# + Refcounts, somehow
# - TODO: Subclasses with staticmethod/classmethod constructors that call parse_proto_into_this


ALL_TEST_CASES: list[tuple[str, FunctionType]] = []


def test_case(fn: FunctionType) -> FunctionType:
    ALL_TEST_CASES.append((fn.__name__, fn))
    return fn


def assert_Primitives_default_values(message: Any):
    assert isinstance(message, pbcc.TestPrimitives)

    assert message.f_int32 == 0
    assert message.f_int64 == 0
    assert message.f_uint32 == 0
    assert message.f_uint64 == 0
    assert message.f_sint32 == 0
    assert message.f_sint64 == 0
    assert message.f_fixed32 == 0
    assert message.f_fixed64 == 0
    assert message.f_sfixed32 == 0
    assert message.f_sfixed64 == 0
    assert message.f_bool is False
    assert message.f_enum1 == pbcc.TestEnum1.TEST_E1_VALUE1
    assert message.f_enum2 == pbcc.TestEnum2.TEST_E2_VALUE1
    assert message.f_float == 0.0
    assert message.f_double == 0.0
    assert message.f_bytes == b""
    assert message.f_string == ""


def assert_ListPrimitives_default_values(message: Any):
    assert isinstance(message, pbcc.TestListPrimitives)

    assert message.f_int32 == []
    assert message.f_int64 == []
    assert message.f_uint32 == []
    assert message.f_uint64 == []
    assert message.f_sint32 == []
    assert message.f_sint64 == []
    assert message.f_fixed32 == []
    assert message.f_fixed64 == []
    assert message.f_sfixed32 == []
    assert message.f_sfixed64 == []
    assert message.f_bool == []
    assert message.f_enum1 == []
    assert message.f_enum2 == []
    assert message.f_float == []
    assert message.f_double == []
    assert message.f_bytes == []
    assert message.f_string == []


def assert_OptionalPrimitives_default_values(message: Any):
    assert isinstance(message, pbcc.TestOptionalPrimitives)

    assert message.f_int32 is None
    assert message.f_int64 is None
    assert message.f_uint32 is None
    assert message.f_uint64 is None
    assert message.f_sint32 is None
    assert message.f_sint64 is None
    assert message.f_fixed32 is None
    assert message.f_fixed64 is None
    assert message.f_sfixed32 is None
    assert message.f_sfixed64 is None
    assert message.f_bool is None
    assert message.f_enum1 is None
    assert message.f_enum2 is None
    assert message.f_float is None
    assert message.f_double is None
    assert message.f_bytes is None
    assert message.f_string is None


def assert_Maps_default_values(message: Any):
    assert isinstance(message, pbcc.TestMaps)

    assert message.f_int32_string == {}
    assert message.f_int64_string == {}
    assert message.f_uint32_string == {}
    assert message.f_uint64_string == {}
    assert message.f_sint32_string == {}
    assert message.f_sint64_string == {}
    assert message.f_fixed32_string == {}
    assert message.f_fixed64_string == {}
    assert message.f_sfixed32_string == {}
    assert message.f_sfixed64_string == {}
    assert message.f_bool_string == {}
    assert message.f_string_string == {}
    assert message.f_string_int32 == {}
    assert message.f_string_int64 == {}
    assert message.f_string_uint32 == {}
    assert message.f_string_uint64 == {}
    assert message.f_string_sint32 == {}
    assert message.f_string_sint64 == {}
    assert message.f_string_fixed32 == {}
    assert message.f_string_fixed64 == {}
    assert message.f_string_sfixed32 == {}
    assert message.f_string_sfixed64 == {}
    assert message.f_string_bool == {}
    assert message.f_string_enum1 == {}
    assert message.f_string_enum2 == {}
    assert message.f_string_float == {}
    assert message.f_string_double == {}
    assert message.f_string_bytes == {}
    assert message.f_string_message == {}


def assert_Submessages_default_values(message: Any):
    assert isinstance(message, pbcc.TestSubmessages)

    assert_Primitives_default_values(message.f_primitives)
    assert_ListPrimitives_default_values(message.f_list_primitives)
    assert_OptionalPrimitives_default_values(message.f_optional_primitives)
    assert_Maps_default_values(message.f_maps)
    assert message.f_optional_msg_primitives is None
    assert message.f_repeated_msg_primitives == []


def assert_Oneofs_default_values(message: Any):
    assert isinstance(message, pbcc.TestOneofs)

    # The default value for a oneof is the type of the first field in the group
    assert message.f_int_or_bytes == 0
    assert message.f_string_or_float == ""
    assert isinstance(message.f_submessage, pbcc.TestPrimitives)
    assert_Primitives_default_values(message.f_submessage)


def check_field_value(
    cls_pb_a: type,  # Google protobuf message constructor
    cls_cc_a: type,  # pbcc message constructor
    field_name_pb: str,
    field_name_cc: str,
    value_cc: Any,
    value_pb: Any,
    enforce_data_equality: bool = True,
) -> None:
    cls_pb = cast(type[Message], cls_pb_a)
    cls_cc = cast(type[PBCCMessage], cls_cc_a)
    if value_cc is not value_pb or field_name_pb != field_name_cc:
        print(
            f"... Checking {cls_cc.__name__}.{field_name_cc} = {value_cc} (PB: {cls_pb.__name__}.{field_name_pb} = {value_pb})"
        )
    else:
        print(f"... Checking {cls_cc.__name__}.{field_name_cc} = {value_cc}")

    # The class should have the attribute set after construction (and it should be the same Python object)
    m_cc = cls_cc(**{field_name_cc: value_cc})
    assert getattr(m_cc, field_name_cc) is value_cc
    # Setting the field after construction should have the same effect
    m_cc = cls_cc()
    setattr(m_cc, field_name_cc, value_cc)
    assert getattr(m_cc, field_name_cc) is value_cc
    # The value should persist through copy
    m_cc2 = m_cc.proto_copy()
    assert getattr(m_cc2, field_name_cc) == value_cc
    del m_cc2
    # The value should be settable through copy
    m_cc = cls_cc().proto_copy(**{field_name_cc: value_cc})
    assert getattr(m_cc, field_name_cc) is value_cc
    # The value should be visible via as_dict()
    m_dict = m_cc.as_dict()
    if hasattr(value_cc, "as_dict"):
        assert m_dict[field_name_cc] == value_cc.as_dict()
    elif isinstance(value_cc, list) and all(hasattr(v, "as_dict") for v in value_cc):
        assert m_dict[field_name_cc] == [v.as_dict() for v in value_cc]
    elif isinstance(value_cc, dict) and all(hasattr(v, "as_dict") for v in value_cc.values()):
        assert m_dict[field_name_cc] == {k: v.as_dict() for k, v in value_cc.items()}
    else:
        assert m_dict[field_name_cc] == value_cc
    # The value should persist through pickle/unpickle, and the entire
    # deserialized message should match the original
    m_cc_pk = pickle.dumps(m_cc)
    m_cc2 = pickle.loads(m_cc_pk)
    assert getattr(m_cc2, field_name_cc) == value_cc
    assert m_cc2 == m_cc

    # The value should persist through pbcc serialize and pbcc deserialize
    data_pbcc = m_cc.as_proto_data()
    m_cc2 = cls_cc.from_proto_data(data_pbcc)
    assert not m_cc2.has_unknown_fields()
    assert getattr(m_cc2, field_name_cc) == value_cc
    # Check equality operator correctness
    assert m_cc2 == m_cc
    assert not (m_cc2 != m_cc)

    # The value should persist through pbcc serialize and pb deserialize
    data_pbcc = m_cc.as_proto_data()
    m_pb = cls_pb.FromString(data_pbcc)
    if value_pb is None:
        assert not m_pb.HasField(field_name_pb)
    elif isinstance(value_pb, list):
        # Google, in their infinite wisdom, has decided that their protobuf
        # implementation shouldn't return normal lists for repeated fields,
        # but instead should return things that look like lists but aren't,
        # in a few important ways. One of those ways is that __eq__ raises
        # if the object is compared to a list. It makes sense to do this
        # when implementing a new list type, but this disregards the more
        # important question: should there even be a separate list type to
        # begin with? Seriously, Google. I just want to compare 5 values.
        assert list(getattr(m_pb, field_name_pb)) == value_pb
    else:
        assert getattr(m_pb, field_name_pb) == value_pb

    # The value should persist through pb serialize and pbcc deserialize
    data_pb = cls_pb(**{field_name_pb: value_pb}).SerializeToString()
    m_cc2 = cls_cc.from_proto_data(data_pb)
    assert not m_cc2.has_unknown_fields()
    assert getattr(m_cc, field_name_cc) == value_cc

    # Google's library and pbcc should return the same data unless
    # there are multiple fields or a dict with multiple keys involved
    if enforce_data_equality:
        assert data_pb == data_pbcc


def check_field_values(
    cls_pb: type,
    cls_cc: type,
    field_name: str,
    allowed_values: Sequence[Any | tuple[Any, Any] | tuple[Any, Any, Any]],
    disallowed_values: Sequence[Any] = (),
    enforce_data_equality: bool = True,
) -> None:
    """Checks serialization, deserialization, and equality for the given
    values in the given field, across both pbcc and Google pb libraries.
    Each value in allowed_values is tested individually. Each entry is one
    of the following:
    - A single value (not a tuple). In this case, the value is used for
        both pbcc and Google pb, and the fields have the same name.
    - A 2-tuple. In this case, the first value is used for pbcc and the
        second is used for Google pb, and the fields have the same name.
    - A 3-tuple. In this case, the first value is the field name in Google
        pb, and the following two fields are treated as in the previous case.
    disallowed_values is a sequence of values that we expect not to be
    valid; we enforce that attempting to serialize any of these in the
    given field raises an exception. Finally, if enforce_data_equality is
    True, we check that the serialized message contents are identical when
    serialized by both pbcc and Google pb. (This is not always enabled
    because it's valid for some types, such as dicts, to serialize values
    in different orders.)"""
    data_pbcc: bytes | None = None
    data_pb: bytes | None = None
    try:
        for values in allowed_values:
            if not isinstance(values, tuple):
                pb_field_name = field_name
                value_cc = value_pb = values
            elif len(values) == 2:
                pb_field_name = field_name
                value_cc = values[0]
                value_pb = values[1]
            elif len(values) == 3:
                values_3 = cast(tuple[Any, Any, Any], values)
                pb_field_name = values_3[0]
                value_cc = values_3[1]
                value_pb = values_3[2]
            else:
                assert False, f"Invalid values format: {values!r}"
            check_field_value(
                cls_pb,
                cls_cc,
                pb_field_name,
                field_name,
                value_cc,
                value_pb,
                enforce_data_equality,
            )

    except Exception:
        # Only show data if the test fails
        if data_pbcc is not None:
            print(f"...   pbcc data: {data_pbcc.hex()}")
        if data_pb is not None:
            print(f"...   pb data: {data_pb.hex()!r}")
        raise

    for value in disallowed_values:
        print(f"... Checking {cls_pb.__name__}.{field_name} = {value}")

        # The class should have the attribute set after construction (and it
        # should be the same Python object). Types aren't checked until
        # serialization, so this should always succeed
        m_cc = cls_cc(**{field_name: value})
        assert getattr(m_cc, field_name) is value
        # Serialization should fail
        try:
            m_cc.as_proto_data()
        except Exception as e:
            print(f"...   Received expected exception: {e}")
        else:
            assert False, (
                f"Expected an exception when setting {field_name}={value} but did not receive one; m_cc={m_cc!r}"
            )


@test_case
def test_subclasses_and_references() -> None:
    class CountedPrimitives(pbcc.TestPrimitives):
        total_count: ClassVar[int] = 0

        def __init__(self, *args, **kwargs) -> None:
            CountedPrimitives.total_count += 1
            super().__init__(*args, **kwargs)

        def __del__(self) -> None:
            CountedPrimitives.total_count -= 1

        def __repr__(self) -> str:
            return "CountedPrimitives+" + pbcc.TestPrimitives.__repr__(self)

    message = CountedPrimitives()
    assert CountedPrimitives.total_count == 1
    assert_Primitives_default_values(message)

    # The message variable is 1 reference, the temporary reference passed to
    # sys.getrefcount is the other
    assert sys.getrefcount(message) == 2

    del message
    assert CountedPrimitives.total_count == 0

    message = CountedPrimitives(f_sint32=24)
    assert CountedPrimitives.total_count == 1
    assert sys.getrefcount(message) == 2
    oneofs = pbcc.TestOneofs(f_submessage=message)
    assert oneofs.f_submessage is message
    assert sys.getrefcount(oneofs) == 2
    assert sys.getrefcount(message) == 3
    del message
    assert CountedPrimitives.total_count == 1
    del oneofs
    assert CountedPrimitives.total_count == 0


@test_case
def test_Primitives() -> None:
    assert_Primitives_default_values(pbcc.TestPrimitives())
    assert_Primitives_default_values(pbcc.TestPrimitives.from_proto_data(b""))
    assert pbcc.TestPrimitives().as_proto_data() == b""

    wrong_types_for_int: tuple[Any, ...] = (
        None,
        "str",
        b"bytes",
        [],
        {},
        pbcc.TestListPrimitives(),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_int32",
        (0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_int64",
        (
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_uint32",
        (0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -1, 0x100000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_uint64",
        (
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x10000000000000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_sint32",
        (0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_sint64",
        (
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_fixed32",
        (0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -1, 0x100000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_fixed64",
        (
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x10000000000000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_sfixed32",
        (0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_sfixed64",
        (
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_bool",
        (True, False),
        (*wrong_types_for_int, pbcc.TestEnum2.TEST_E2_VALUE3),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_enum1",
        (
            (pbcc.TestEnum1.TEST_E1_VALUE1, pb.TEST_E1_VALUE1),
            (pbcc.TestEnum1.TEST_E1_VALUE2, pb.TEST_E1_VALUE2),
            (pbcc.TestEnum1.TEST_E1_VALUE3, pb.TEST_E1_VALUE3),
        ),
        (
            None,
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestListPrimitives(),
            0,
        ),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_enum2",
        (
            (pbcc.TestEnum2.TEST_E2_VALUE1, pb.TEST_E2_VALUE1),
            (pbcc.TestEnum2.TEST_E2_VALUE2, pb.TEST_E2_VALUE2),
            (pbcc.TestEnum2.TEST_E2_VALUE3, pb.TEST_E2_VALUE3),
        ),
        (
            None,
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum1.TEST_E1_VALUE3,
            pbcc.TestListPrimitives(),
            0,
        ),
    )
    valid_floats = (
        0.0,
        1.0,
        -1.0,
        float("inf"),
        -float("inf"),
        0,
        -1,
        1,
        pbcc.TestEnum2.TEST_E2_VALUE3,
    )
    invalid_floats: tuple[Any, ...] = (
        None,
        "str",
        b"bytes",
        [],
        {},
        pbcc.TestListPrimitives(),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_float",
        valid_floats,
        invalid_floats,
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_double",
        valid_floats,
        invalid_floats,
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_bytes",
        (b"", b"short bytes", b"long bytes" * 8192),
        (
            None,
            7,
            3.0,
            "str",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestListPrimitives(),
        ),
    )
    check_field_values(
        pb.TestPrimitives,
        pbcc.TestPrimitives,
        "f_string",
        ("", "short string", "long string" * 8192, "n₀n→åscíï striñg"),
        (
            None,
            7,
            3.0,
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestListPrimitives(),
        ),
    )


@test_case
def test_ListPrimitives() -> None:
    assert_ListPrimitives_default_values(pbcc.TestListPrimitives())
    assert_ListPrimitives_default_values(pbcc.TestListPrimitives.from_proto_data(b""))
    assert pbcc.TestListPrimitives().as_proto_data() == b""

    wrong_types: tuple[Any, ...] = (
        None,
        1,
        5.0,
        True,
        "str",
        b"bytes",
        [[]],
        {},
        pbcc.TestEnum2.TEST_E2_VALUE3,
        pbcc.TestListPrimitives(),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_int32",
        ([], [0], [1, 0x7FFFFFFF, -1, -0x80000000]),
        (*wrong_types, [None], [-0x80000001], [0x80000000], [0, 1, 2, 0x80000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_int64",
        ([], [0], [1, 0x7FFFFFFFFFFFFFFF, -1, -0x8000000000000000]),
        (
            *wrong_types,
            [None],
            [-0x8000000000000001],
            [0x8000000000000000],
            [0, 1, 2, 0x8000000000000000],
        ),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_uint32",
        ([], [0], [1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF]),
        (*wrong_types, [None], [-1], [0x100000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_uint64",
        ([], [0], [1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF]),
        (*wrong_types, [None], [-1], [0x10000000000000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_sint32",
        ([], [0], [1, 0x7FFFFFFF, -1, -0x80000000]),
        (*wrong_types, [None], [-0x80000001], [0x80000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_sint64",
        ([], [0], [1, 0x7FFFFFFFFFFFFFFF, -1, -0x8000000000000000]),
        (*wrong_types, [None], [-0x8000000000000001], [0x8000000000000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_fixed32",
        ([], [0], [1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF]),
        (*wrong_types, [None], [-1], [0x100000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_fixed64",
        ([], [0], [1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF]),
        (*wrong_types, [None], [-1], [0x10000000000000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_sfixed32",
        ([], [0], [1, 0x7FFFFFFF, -1, -0x80000000]),
        (*wrong_types, [None], [-0x80000001], [0x80000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_sfixed64",
        ([], [0], [1, 0x7FFFFFFFFFFFFFFF, -1, -0x8000000000000000]),
        (*wrong_types, [None], [-0x8000000000000001], [0x8000000000000000]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_bool",
        ([], [True], [False], [True, False, False, True, False]),
        (*wrong_types, [True, False, 2, False]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_enum1",
        (
            [],
            ([pbcc.TestEnum1.TEST_E1_VALUE1], [pb.TEST_E1_VALUE1]),
            (
                [pbcc.TestEnum1.TEST_E1_VALUE2, pbcc.TestEnum1.TEST_E1_VALUE3],
                [pb.TEST_E1_VALUE2, pb.TEST_E1_VALUE3],
            ),
        ),
        (*wrong_types, [0], [pbcc.TestEnum2.TEST_E2_VALUE1]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_enum2",
        (
            [],
            ([pbcc.TestEnum2.TEST_E2_VALUE1], [pb.TEST_E2_VALUE1]),
            (
                [pbcc.TestEnum2.TEST_E2_VALUE2, pbcc.TestEnum2.TEST_E2_VALUE3],
                [pb.TEST_E2_VALUE2, pb.TEST_E2_VALUE3],
            ),
        ),
        (*wrong_types, [0], [pbcc.TestEnum1.TEST_E1_VALUE3]),
    )
    valid_floats: tuple[list[Any], ...] = (
        [],
        [0.0],
        [1.0, -1.0, float("inf"), -float("inf"), 0, -1, 1],
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_float",
        valid_floats,
        wrong_types,
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_double",
        valid_floats,
        wrong_types,
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_bytes",
        ([], [b""], [b"short bytes", b"long bytes" * 8192]),
        (*wrong_types, [b"bytes", b"bytes", "oops, a str", b"more bytes"]),
    )
    check_field_values(
        pb.TestListPrimitives,
        pbcc.TestListPrimitives,
        "f_string",
        ([], [""], ["short string", "long string" * 8192, "n₀n→åscíï striñg"]),
        (*wrong_types, ["str", "str", b"oops, a bytes", "more str"]),
    )


@test_case
def test_OptionalPrimitives() -> None:
    assert_OptionalPrimitives_default_values(pbcc.TestOptionalPrimitives())
    assert_OptionalPrimitives_default_values(pbcc.TestOptionalPrimitives.from_proto_data(b""))
    assert pbcc.TestOptionalPrimitives().as_proto_data() == b""

    wrong_types_for_int: tuple[Any, ...] = (
        "str",
        b"bytes",
        [],
        {},
        pbcc.TestListPrimitives(),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_int32",
        (None, 0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_int64",
        (
            None,
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_uint32",
        (
            None,
            0,
            1,
            0x7FFFFFFF,
            0x80000000,
            0xFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x100000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_uint64",
        (
            None,
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x10000000000000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_sint32",
        (None, 0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_sint64",
        (
            None,
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_fixed32",
        (
            None,
            0,
            1,
            0x7FFFFFFF,
            0x80000000,
            0xFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x100000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_fixed64",
        (
            None,
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -1, 0x10000000000000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_sfixed32",
        (None, 0, 1, 0x7FFFFFFF, -1, -0x80000000, pbcc.TestEnum2.TEST_E2_VALUE3),
        (*wrong_types_for_int, -0x80000001, 0x80000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_sfixed64",
        (
            None,
            0,
            1,
            0x7FFFFFFFFFFFFFFF,
            -1,
            -0x8000000000000000,
            pbcc.TestEnum2.TEST_E2_VALUE3,
        ),
        (*wrong_types_for_int, -0x8000000000000001, 0x8000000000000000),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_bool",
        (None, True, False),
        (*wrong_types_for_int, pbcc.TestEnum2.TEST_E2_VALUE1),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_enum1",
        (
            None,
            (pbcc.TestEnum1.TEST_E1_VALUE1, pb.TEST_E1_VALUE1),
            (pbcc.TestEnum1.TEST_E1_VALUE2, pb.TEST_E1_VALUE2),
            (pbcc.TestEnum1.TEST_E1_VALUE3, pb.TEST_E1_VALUE3),
        ),
        (
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE1,  # pbcc checks the actual enum type; Google pb doesn't
            pbcc.TestListPrimitives(),
            0,
        ),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_enum2",
        (
            None,
            (pbcc.TestEnum2.TEST_E2_VALUE1, pb.TEST_E2_VALUE1),
            (pbcc.TestEnum2.TEST_E2_VALUE2, pb.TEST_E2_VALUE2),
            (pbcc.TestEnum2.TEST_E2_VALUE3, pb.TEST_E2_VALUE3),
        ),
        (
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum1.TEST_E1_VALUE3,  # pbcc checks the actual enum type; Google pb doesn't
            pbcc.TestListPrimitives(),
            0,
        ),
    )
    valid_floats = (
        None,
        0.0,
        1.0,
        -1.0,
        float("inf"),
        -float("inf"),
        0,
        -1,
        1,
        pbcc.TestEnum1.TEST_E1_VALUE3,
    )
    invalid_floats: tuple[Any, ...] = (
        "str",
        b"bytes",
        [],
        {},
        pbcc.TestListPrimitives(),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_float",
        valid_floats,
        invalid_floats,
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_double",
        valid_floats,
        invalid_floats,
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_bytes",
        (None, b"", b"short bytes", b"long bytes " * 2048),
        (
            7,
            3.0,
            "str",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestListPrimitives(),
        ),
    )
    check_field_values(
        pb.TestOptionalPrimitives,
        pbcc.TestOptionalPrimitives,
        "f_string",
        (None, "", "short string", "long string " * 2048, "n₀n→åscíï striñg"),
        (
            7,
            3.0,
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestListPrimitives(),
        ),
    )


@test_case
def test_Maps() -> None:
    assert_Maps_default_values(pbcc.TestMaps())
    assert_Maps_default_values(pbcc.TestMaps.from_proto_data(b""))
    assert pbcc.TestMaps().as_proto_data() == b""

    wrong_types: tuple[Any, ...] = (
        None,
        1,
        5.0,
        True,
        "str",
        b"bytes",
        [],
        pbcc.TestEnum2.TEST_E2_VALUE3,
        pbcc.TestListPrimitives(),
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_int32_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x80000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_int32_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_int64_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x8000000000000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_int64_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_uint32_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x100000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_uint32_string",
        ({0: "str", 1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_uint64_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x10000000000000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_uint64_string",
        ({0: "str", 1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sint32_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x80000000: "key too big"},
            {-0x80000001: "key too small"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sint32_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sint64_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x8000000000000000: "key too big"},
            {-0x8000000000000001: "key too small"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sint64_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_fixed32_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x100000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_fixed32_string",
        ({0: "str", 1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_fixed64_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x10000000000000000: "key too big"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_fixed64_string",
        ({0: "str", 1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sfixed32_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x80000000: "key too big"},
            {-0x80000001: "key too small"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sfixed32_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sfixed64_string",
        ({}, {0: ""}),
        (
            *wrong_types,
            {0: None},
            {0x8000000000000000: "key too big"},
            {-0x8000000000000001: "key too small"},
            {30: b"wrong value type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_sfixed64_string",
        ({0: "str", -1: "omg", 2: "hax"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_bool_string",
        ({}, {True: "troo"}, {False: "falz"}),
        (
            *wrong_types,
            {0: None},
            {1: "key has to be an actual bool, not 0 or 1"},
            {30: b"wrong value type"},
            {pbcc.TestEnum1.TEST_E1_VALUE2: "key is the right type but still out of range"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_bool_string",
        ({True: "treu", False: "faults"},),
        (),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_string",
        ({}, {"now we're getting to": "the fun stuff finally"}),
        (
            *wrong_types,
            {b"wrong key type": "right value type, but not good enough"},
            {"right key type": b"but wrong value type"},
            {pbcc.TestEnum1.TEST_E1_VALUE2: "wrong key type"},
        ),
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_string",
        ({"this": "dict", "has": "more", "keys": "than", "the": "above"},),
        (),
        enforce_data_equality=False,
    )

    # We can be a bit lazier in checking the different value types, since the
    # above cases already cover a lot of ground
    check_field_values(pb.TestMaps, pbcc.TestMaps, "f_string_string", ({},), ())

    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_int32",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_int64",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_uint32",
        ({}, {"key1": 35, "key2": 27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_uint64",
        ({}, {"key1": 35, "key2": 27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_sint32",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_sint64",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_fixed32",
        ({}, {"key1": 35, "key2": 27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_fixed64",
        ({}, {"key1": 35, "key2": 27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_sfixed32",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_sfixed64",
        ({}, {"key1": 35, "key2": -27, "key3": 0}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_bool",
        ({}, {"key1": True, "key2": False, "key3": False}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_enum1",
        (
            {},
            (
                {
                    "key1": pbcc.TestEnum1.TEST_E1_VALUE1,
                    "key2": pbcc.TestEnum1.TEST_E1_VALUE2,
                    "key3": pbcc.TestEnum1.TEST_E1_VALUE3,
                },
                {
                    "key1": pb.TEST_E1_VALUE1,
                    "key2": pb.TEST_E1_VALUE2,
                    "key3": pb.TEST_E1_VALUE3,
                },
            ),
        ),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_enum2",
        (
            {},
            (
                {
                    "key1": pbcc.TestEnum2.TEST_E2_VALUE1,
                    "key2": pbcc.TestEnum2.TEST_E2_VALUE2,
                    "key3": pbcc.TestEnum2.TEST_E2_VALUE3,
                },
                {
                    "key1": pb.TEST_E2_VALUE1,
                    "key2": pb.TEST_E2_VALUE2,
                    "key3": pb.TEST_E2_VALUE3,
                },
            ),
        ),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_float",
        ({}, {"key1": -1.0, "key2": 0.0, "key3": float("inf")}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_double",
        ({}, {"key1": -1.0, "key2": 0.0, "key3": float("inf")}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_bytes",
        ({}, {"key1": b"donut", "key2": b"bear claw", "key3": b"croissant"}),
        (),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestMaps,
        pbcc.TestMaps,
        "f_string_message",
        (
            {},
            (
                {
                    "key1": pbcc.TestPrimitives(f_int64=2),
                    "key2": pbcc.TestPrimitives(f_double=5.0),
                    "key3": pbcc.TestPrimitives(f_bytes=b"bytes"),
                },
                {
                    "key1": pb.TestPrimitives(f_int64=2),
                    "key2": pb.TestPrimitives(f_double=5.0),
                    "key3": pb.TestPrimitives(f_bytes=b"bytes"),
                },
            ),
        ),
        (),
        enforce_data_equality=False,
    )


@test_case
def test_Submessages() -> None:
    assert_Submessages_default_values(pbcc.TestSubmessages())
    assert_Submessages_default_values(pbcc.TestSubmessages.from_proto_data(b""))
    assert pbcc.TestSubmessages().as_proto_data() == b""

    wrong_types: tuple[Any, ...] = (
        None,
        1,
        5.0,
        True,
        "str",
        b"bytes",
        [],
        {},
        pbcc.TestEnum2.TEST_E2_VALUE3,
        pbcc.TestSubmessages(),
    )

    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_primitives",
        (
            (pbcc.TestPrimitives(f_sint32=-32), pb.TestPrimitives(f_sint32=-32)),
            (
                pbcc.TestPrimitives(f_enum1=pbcc.TestEnum1.TEST_E1_VALUE2),
                pb.TestPrimitives(f_enum1=pb.TEST_E1_VALUE2),
            ),
        ),
        wrong_types,
    )
    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_primitives",
        (
            (
                pbcc.TestPrimitives(
                    f_sint32=-32,
                    f_uint64=64,
                    f_enum2=pbcc.TestEnum2.TEST_E2_VALUE2,
                    f_bytes=b"bites",
                ),
                pb.TestPrimitives(
                    f_sint32=-32,
                    f_uint64=64,
                    f_enum2=pb.TEST_E2_VALUE2,
                    f_bytes=b"bites",
                ),
            ),
            (
                pbcc.TestPrimitives(
                    f_float=-6.0,
                    f_bool=False,
                    f_enum1=pbcc.TestEnum1.TEST_E1_VALUE2,
                    f_string="strung",
                ),
                pb.TestPrimitives(
                    f_float=-6.0,
                    f_bool=False,
                    f_enum1=pb.TEST_E1_VALUE2,
                    f_string="strung",
                ),
            ),
        ),
        wrong_types,
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_list_primitives",
        (
            (
                pbcc.TestListPrimitives(
                    f_sint32=[-32, 68],
                    f_uint64=[64, 0, 1843],
                    f_enum2=[pbcc.TestEnum2.TEST_E2_VALUE2],
                    f_bytes=[b"bites"],
                ),
                pb.TestListPrimitives(
                    f_sint32=[-32, 68],
                    f_uint64=[64, 0, 1843],
                    f_enum2=[pb.TEST_E2_VALUE2],
                    f_bytes=[b"bites"],
                ),
            ),
            (
                pbcc.TestListPrimitives(
                    f_float=[-6.0, 2.0],
                    f_bool=[False, True],
                    f_enum1=[pbcc.TestEnum1.TEST_E1_VALUE2],
                    f_string=["strung"],
                ),
                pb.TestListPrimitives(
                    f_float=[-6.0, 2.0],
                    f_bool=[False, True],
                    f_enum1=[pb.TEST_E1_VALUE2],
                    f_string=["strung"],
                ),
            ),
        ),
        wrong_types,
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_string_primitives",
        (
            (
                {"seven": pbcc.TestPrimitives(f_int64=5)},
                {"seven": pb.TestPrimitives(f_int64=5)},
            ),
            (
                {"eight": pbcc.TestPrimitives(f_string="str")},
                {"eight": pb.TestPrimitives(f_string="str")},
            ),
        ),
        (
            None,
            1,
            5.0,
            True,
            "str",
            b"bytes",
            [],
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestSubmessages(),
        ),
        enforce_data_equality=False,
    )

    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_optional_msg_primitives",
        (
            None,
            (pbcc.TestPrimitives(f_int64=5), pb.TestPrimitives(f_int64=5)),
        ),
        (
            1,
            5.0,
            True,
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestSubmessages(),
        ),
        enforce_data_equality=False,
    )
    check_field_values(
        pb.TestSubmessages,
        pbcc.TestSubmessages,
        "f_repeated_msg_primitives",
        (
            ([pbcc.TestPrimitives(f_int64=5)], [pb.TestPrimitives(f_int64=5)]),
            (
                [pbcc.TestPrimitives(f_string="str")],
                [pb.TestPrimitives(f_string="str")],
            ),
        ),
        (
            None,
            1,
            5.0,
            True,
            "str",
            b"bytes",
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestSubmessages(),
        ),
        enforce_data_equality=False,
    )


@test_case
def test_Oneofs() -> None:
    assert_Oneofs_default_values(pbcc.TestOneofs())
    assert_Oneofs_default_values(pbcc.TestOneofs.from_proto_data(b""))
    assert pbcc.TestOneofs().as_proto_data() == b""

    check_field_values(
        pb.TestOneofs,
        pbcc.TestOneofs,
        "f_int_or_bytes",
        (
            ("f_int", 500, 500),
            ("f_bytes", b"bights", b"bights"),
            ("f_int", pbcc.TestEnum2.TEST_E2_VALUE3, 1),
        ),
        (None, "str", [], {}, pbcc.TestPrimitives()),
    )
    check_field_values(
        pb.TestOneofs,
        pbcc.TestOneofs,
        "f_string_or_float",
        (
            ("f_string", "strong", "strong"),
            ("f_float", 3.0, 3.0),
            ("f_float", pbcc.TestEnum2.TEST_E2_VALUE3, 1.0),
        ),
        (None, b"bytes", [], {}, pbcc.TestOneofs()),
    )
    check_field_values(
        pb.TestOneofs,
        pbcc.TestOneofs,
        "f_submessage",
        (
            (
                "f_primitives",
                pbcc.TestPrimitives(f_bytes=b"baits"),
                pb.TestPrimitives(f_bytes=b"baits"),
            ),
            (
                "f_list_primitives",
                pbcc.TestListPrimitives(f_bool=[True, False, True]),
                pb.TestListPrimitives(f_bool=[True, False, True]),
            ),
            (
                "f_optional_primitives",
                pbcc.TestOptionalPrimitives(f_sint64=-432),
                pb.TestOptionalPrimitives(f_sint64=-432),
            ),
        ),
        (
            None,
            1,
            5.0,
            True,
            "str",
            b"bytes",
            [],
            {},
            pbcc.TestEnum2.TEST_E2_VALUE3,
            pbcc.TestOneofs(),
            pbcc.TestPrimitives(f_bytes="not a bytes object"),  # type: ignore
        ),
    )  # type: ignore


def assert_parsing_fails(cls: type, data: bytes, expected_message_str: str | None = None) -> None:
    cls = cast(type[PBCCMessage], cls)
    try:
        cls.from_proto_data(data)
    except Exception as e:
        assert expected_message_str is None or expected_message_str in repr(e), (
            f"Incorrect exception raised with data {data.hex()}; expected {expected_message_str!r}, received {e}"
        )
        print(f"... ({data.hex().upper()}) Received expected exception: {e}")
    else:
        assert False, f"Expected an exception with data {data.hex()} but did not receive one"


@test_case
def test_deserialize_wrong_field_types() -> None:
    data = bytes.fromhex(
        "".join(
            [
                "08 03",  # f_int32 = 3
                "10 03",  # f_int64 = 3
                "18 03",  # f_uint32 = 3
                "20 03",  # f_uint64 = 3
                "28 06",  # f_sint32 = 3
                "30 06",  # f_sint64 = 3
                "3D 03000000",  # f_fixed32 = 3
                "41 0300000000000000",  # f_fixed64 = 3
                "4D 03000000",  # f_sfixed32 = 3
                "51 0300000000000000",  # f_sfixed64 = 3
                "58 01",  # f_bool = True
                "60 0A",  # f_enum1 = pbcc.TestEnum1.TEST_E1_VALUE3
                "68 01",  # f_enum2 = pbcc.TestEnum2.TEST_E2_VALUE3
                "75 00004040",  # f_float = 3.0
                "79 0000000000000840",  # f_double = 3.0
                "8201 03 333333",  # f_bytes = b'333'
                "8A01 03 333333",  # f_string = '333'
            ]
        )
    )
    source_msg = pbcc.TestPrimitives(
        f_int32=3,
        f_int64=3,
        f_uint32=3,
        f_uint64=3,
        f_sint32=3,
        f_sint64=3,
        f_fixed32=3,
        f_fixed64=3,
        f_sfixed32=3,
        f_sfixed64=3,
        f_bool=True,
        f_enum1=pbcc.TestEnum1.TEST_E1_VALUE3,
        f_enum2=pbcc.TestEnum2.TEST_E2_VALUE3,
        f_float=3.0,
        f_double=3.0,
        f_bytes=b"333",
        f_string="333",
    )
    msg = pbcc.TestPrimitives.from_proto_data(data)
    assert not msg.has_unknown_fields()
    assert msg == source_msg
    msg = pbcc.TestPrimitives.from_proto_data(data, ignore_incorrect_types=True)
    assert not msg.has_unknown_fields()
    assert msg == source_msg

    for retain_unknown_fields in (True, False):
        # All fields as VARINT
        data = bytes.fromhex(
            "".join(
                [
                    "08 03",  # f_int32 = 3
                    "10 03",  # f_int64 = 3
                    "18 03",  # f_uint32 = 3
                    "20 03",  # f_uint64 = 3
                    "28 06",  # f_sint32 = 3
                    "30 06",  # f_sint64 = 3
                    "38 03",  # Wrong type
                    "40 03",  # Wrong type
                    "48 03",  # Wrong type
                    "50 03",  # Wrong type
                    "58 01",  # f_bool = True
                    "60 0A",  # f_enum1 = pbcc.TestEnum1.TEST_E1_VALUE3
                    "68 01",  # f_enum2 = pbcc.TestEnum2.TEST_E2_VALUE3
                    "70 03",  # Wrong type
                    "78 03",  # Wrong type
                    "8001 03",  # Wrong type
                    "8801 03",  # Wrong type
                ]
            )
        )
        msg = pbcc.TestPrimitives.from_proto_data(
            data,
            retain_unknown_fields=retain_unknown_fields,
            ignore_incorrect_types=True,
        )
        assert msg.has_unknown_fields() == retain_unknown_fields
        assert msg == pbcc.TestPrimitives(
            f_int32=3,
            f_int64=3,
            f_uint32=3,
            f_uint64=3,
            f_sint32=3,
            f_sint64=3,
            f_bool=True,
            f_enum1=pbcc.TestEnum1.TEST_E1_VALUE3,
            f_enum2=pbcc.TestEnum2.TEST_E2_VALUE3,
        )

        # All fields as INT64
        data = bytes.fromhex(
            "".join(
                [
                    "09 0300000000000000",  # Wrong type
                    "11 0300000000000000",  # Wrong type
                    "19 0300000000000000",  # Wrong type
                    "21 0300000000000000",  # Wrong type
                    "29 0600000000000000",  # Wrong type
                    "31 0600000000000000",  # Wrong type
                    "39 0300000000000000",  # Wrong type
                    "41 0300000000000000",  # f_fixed64 = 3
                    "49 0300000000000000",  # Wrong type
                    "51 0300000000000000",  # f_sfixed64 = 3
                    "59 0100000000000000",  # Wrong type
                    "61 0A00000000000000",  # Wrong type
                    "69 0100000000000000",  # Wrong type
                    "71 0000404000000000",  # Wrong type
                    "79 0000000000000840",  # f_double = 3.0
                    "8101 0300000000000000",  # Wrong type
                    "8901 0300000000000000",  # Wrong type
                ]
            )
        )
        msg = pbcc.TestPrimitives.from_proto_data(
            data,
            retain_unknown_fields=retain_unknown_fields,
            ignore_incorrect_types=True,
        )
        assert msg.has_unknown_fields() == retain_unknown_fields
        assert msg == pbcc.TestPrimitives(f_fixed64=3, f_sfixed64=3, f_double=3.0)

        # All fields as LENGTH
        data = bytes.fromhex(
            "".join(
                [
                    "0A 03 333333",  # Wrong type
                    "12 03 333333",  # Wrong type
                    "1A 03 333333",  # Wrong type
                    "22 03 333333",  # Wrong type
                    "2A 03 333333",  # Wrong type
                    "32 03 333333",  # Wrong type
                    "3A 03 333333",  # Wrong type
                    "42 03 333333",  # Wrong type
                    "4A 03 333333",  # Wrong type
                    "52 03 333333",  # Wrong type
                    "5A 03 333333",  # Wrong type
                    "62 03 333333",  # Wrong type
                    "6A 03 333333",  # Wrong type
                    "72 03 333333",  # Wrong type
                    "7A 03 333333",  # Wrong type
                    "8201 03 333333",  # f_bytes = b'333'
                    "8A01 03 333333",  # f_string = '333'
                ]
            )
        )
        msg = pbcc.TestPrimitives.from_proto_data(
            data,
            retain_unknown_fields=retain_unknown_fields,
            ignore_incorrect_types=True,
        )
        assert msg.has_unknown_fields() == retain_unknown_fields
        assert msg == pbcc.TestPrimitives(f_bytes=b"333", f_string="333")

        # All fields as INT32
        data = bytes.fromhex(
            "".join(
                [
                    "0D 03000000",  # Wrong type
                    "15 03000000",  # Wrong type
                    "1D 03000000",  # Wrong type
                    "25 03000000",  # Wrong type
                    "2D 06000000",  # Wrong type
                    "35 06000000",  # Wrong type
                    "3D 03000000",  # f_fixed32 = 3
                    "45 03000000",  # Wrong type
                    "4D 03000000",  # f_sfixed32 = 3
                    "55 03000000",  # Wrong type
                    "5D 01000000",  # Wrong type
                    "65 0A000000",  # Wrong type
                    "6D 01000000",  # Wrong type
                    "75 00004040",  # f_float = 3.0
                    "7D 00000040",  # Wrong type
                    "8501 03000000",  # Wrong type
                    "8D01 03000000",  # Wrong type
                ]
            )
        )
        msg = pbcc.TestPrimitives.from_proto_data(
            data,
            retain_unknown_fields=retain_unknown_fields,
            ignore_incorrect_types=True,
        )
        assert msg.has_unknown_fields() == retain_unknown_fields
        assert msg == pbcc.TestPrimitives(f_fixed32=3, f_sfixed32=3, f_float=3.0)

        # Repeated fields should ignore any individual entry if its type is
        # wrong. We'll test all cases at once by passing the same list of
        # [varint(3), int64(3), bytes('333'), int32(3)] for each field (though
        # for the float fields the data will be IEEE754-encoded)
        data = bytes.fromhex(
            "".join(
                [
                    "08 03 09 0300000000000000 0A 03 040506 0D 03000000",  # f_int32
                    "10 03 11 0300000000000000 12 03 040506 15 03000000",  # f_int64
                    "18 03 19 0300000000000000 1A 03 040506 1D 03000000",  # f_uint32
                    "20 03 21 0300000000000000 22 03 040506 25 03000000",  # f_uint64
                    "28 06 29 0600000000000000 2A 03 080A0C 2D 06000000",  # f_sint32
                    "30 06 31 0600000000000000 32 03 080A0C 35 06000000",  # f_sint64
                    "38 03 39 0300000000000000 3A 0C 04000000 05000000 06000000 3D 03000000",  # f_fixed32
                    "40 03 41 0300000000000000 42 18 0400000000000000 0500000000000000 0600000000000000 45 03000000",  # f_fixed64
                    "48 03 49 0300000000000000 4A 0C 04000000 05000000 06000000 4D 03000000",  # f_sfixed32
                    "50 03 51 0300000000000000 52 18 0400000000000000 0500000000000000 0600000000000000 55 03000000",  # f_sfixed64
                    "58 01 59 0100000000000000 5A 03 010001 5D 01000000",  # f_bool
                    "60 0A 61 0A00000000000000 62 03 000A05 65 0A000000",  # f_enum1
                    "68 01 69 0100000000000000 6A 03 000100 6D 01000000",  # f_enum2
                    "70 03 71 0000000000000840 72 08 00000000 0000803F 75 00004040",  # f_float
                    "78 03 79 0000000000000840 7A 10 0000000000000000 000000000000F03F 7D 00004040",  # f_double
                    "8001 03 8101 0000000000000840 8201 03 333333 8501 00004040",  # f_bytes
                    "8801 03 8901 0000000000000840 8A01 03 333333 8D01 00004040",  # f_string
                ]
            )
        )
        source_msg2 = pbcc.TestListPrimitives(
            f_int32=[3, 4, 5, 6],
            f_int64=[3, 4, 5, 6],
            f_uint32=[3, 4, 5, 6],
            f_uint64=[3, 4, 5, 6],
            f_sint32=[3, 4, 5, 6],
            f_sint64=[3, 4, 5, 6],
            f_fixed32=[4, 5, 6, 3],
            f_fixed64=[3, 4, 5, 6],
            f_sfixed32=[4, 5, 6, 3],
            f_sfixed64=[3, 4, 5, 6],
            f_bool=[True, True, False, True],
            f_enum1=[
                pbcc.TestEnum1.TEST_E1_VALUE3,
                pbcc.TestEnum1.TEST_E1_VALUE1,
                pbcc.TestEnum1.TEST_E1_VALUE3,
                pbcc.TestEnum1.TEST_E1_VALUE2,
            ],
            f_enum2=[
                pbcc.TestEnum2.TEST_E2_VALUE3,
                pbcc.TestEnum2.TEST_E2_VALUE1,
                pbcc.TestEnum2.TEST_E2_VALUE3,
                pbcc.TestEnum2.TEST_E2_VALUE1,
            ],
            f_float=[0.0, 1.0, 3.0],
            f_double=[3.0, 0.0, 1.0],
            f_bytes=[b"333"],
            f_string=["333"],
        )
        msg2 = pbcc.TestListPrimitives.from_proto_data(
            data,
            retain_unknown_fields=retain_unknown_fields,
            ignore_incorrect_types=True,
        )
        assert msg2.has_unknown_fields() == retain_unknown_fields
        assert msg2 == source_msg2

        # TODO: Maps don't behave the same way as lists; if a map key or value
        # is the wrong type, it always raises. It would be nice to make
        # ignore_incorrect_types work for maps too, but this would break
        # abstraction in pymodule.cc.
        for test_case in (
            "0A 0E  09 0300000000000000 12 03 333333",  # f_int32_string wrong key type
            "0A 08  0A 01 03 12 03 333333",  # f_int32_string wrong key type
            "0A 0A  0D 03000000 12 03 333333",  # f_int32_string wrong key type
            "0A 04  08 03 10 03",  # f_int32_string wrong value type
            "0A 07  08 03 11 03000000",  # f_int32_string wrong value type
            "0A 0B  08 03 15 0300000000000000",  # f_int32_string wrong value type
            "12 0E  09 0300000000000000 12 03 333333",  # f_int64_string wrong key type
            "12 08  0A 01 03 12 03 333333",  # f_int64_string wrong key type
            "12 0A  0D 03000000 12 03 333333",  # f_int64_string wrong key type
            "12 04  08 03 10 03",  # f_int64_string wrong value type
            "12 07  08 03 11 03000000",  # f_int64_string wrong value type
            "12 0B  08 03 15 0300000000000000",  # f_int64_string wrong value type
            "1A 0E  09 0300000000000000 12 03 333333",  # f_uint32_string wrong key type
            "1A 08  0A 01 03 12 03 333333",  # f_uint32_string wrong key type
            "1A 0A  0D 03000000 12 03 333333",  # f_uint32_string wrong key type
            "1A 04  08 03 10 03",  # f_uint32_string wrong value type
            "1A 07  08 03 11 03000000",  # f_uint32_string wrong value type
            "1A 0B  08 03 15 0300000000000000",  # f_uint32_string wrong value type
            "22 0E  09 0300000000000000 12 03 333333",  # f_uint64_string wrong key type
            "22 08  0A 01 03 12 03 333333",  # f_uint64_string wrong key type
            "22 0A  0D 03000000 12 03 333333",  # f_uint64_string wrong key type
            "22 04  08 03 10 03",  # f_uint64_string wrong value type
            "22 07  08 03 11 03000000",  # f_uint64_string wrong value type
            "22 0B  08 03 15 0300000000000000",  # f_uint64_string wrong value type
            "2A 0E  09 0600000000000000 12 03 333333",  # f_sint32_string wrong key type
            "2A 08  0A 01 06 12 03 333333",  # f_sint32_string wrong key type
            "2A 0A  0D 06000000 12 03 333333",  # f_sint32_string wrong key type
            "2A 04  08 06 10 03",  # f_sint32_string wrong key type
            "2A 07  08 06 11 03000000",  # f_sint32_string wrong key type
            "2A 0B  08 06 15 0300000000000000",  # f_sint32_string wrong key type
            "32 0E  09 0600000000000000 12 03 333333",  # f_sint64_string wrong key type
            "32 08  0A 01 06 12 03 333333",  # f_sint64_string wrong key type
            "32 0A  0D 06000000 12 03 333333",  # f_sint64_string wrong key type
            "32 04  08 06 10 03",  # f_sint64_string wrong key type
            "32 07  08 06 11 03000000",  # f_sint64_string wrong key type
            "32 0B  08 06 15 0300000000000000",  # f_sint64_string wrong key type
            "3A 07  08 03 12 03 333333",  # f_fixed32_string wrong key type
            "3A 0E  09 0300000000000000 12 03 333333",  # f_fixed32_string wrong key type
            "3A 08  0A 01 03 12 03 333333",  # f_fixed32_string wrong key type
            "3A 07  0D 03000000 10 03",  # f_fixed32_string wrong value type
            "3A 0A  0D 03000000 11 03000000",  # f_fixed32_string wrong value type
            "3A 0E  0D 03000000 15 0300000000000000",  # f_fixed32_string wrong value type
            "42 07  08 03 12 03 333333",  # f_fixed64_string wrong key type
            "42 08  0A 01 03 12 03 333333",  # f_fixed64_string wrong key type
            "42 0A  0D 03000000 12 03 333333",  # f_fixed64_string wrong key type
            "42 0B  09 0300000000000000 10 03",  # f_fixed64_string wrong value type
            "42 0E  09 0300000000000000 11 03000000",  # f_fixed64_string wrong value type
            "42 12  09 0300000000000000 15 0300000000000000",  # f_fixed64_string wrong value type
            "4A 07  08 03 12 03 333333",  # f_sfixed32_string wrong key type
            "4A 0E  09 0300000000000000 12 03 333333",  # f_sfixed32_string wrong key type
            "4A 08  0A 01 03 12 03 333333",  # f_sfixed32_string wrong key type
            "4A 07  0D 03000000 10 03",  # f_sfixed32_string wrong value type
            "4A 0A  0D 03000000 11 03000000",  # f_sfixed32_string wrong value type
            "4A 0E  0D 03000000 15 0300000000000000",  # f_sfixed32_string wrong value type
            "52 07  08 03 12 03 333333",  # f_sfixed64_string wrong key type
            "52 08  0A 01 03 12 03 333333",  # f_sfixed64_string wrong key type
            "52 0A  0D 03000000 12 03 333333",  # f_sfixed64_string wrong key type
            "52 0B  09 0300000000000000 10 03",  # f_sfixed64_string wrong value type
            "52 0E  09 0300000000000000 11 03000000",  # f_sfixed64_string wrong value type
            "52 12  09 0300000000000000 15 0300000000000000",  # f_sfixed64_string wrong value type
            "5A 0E  09 0100000000000000 12 03 333333",  # f_bool_string wrong key type
            "5A 08  0A 01 01 12 03 333333",  # f_bool_string wrong key type
            "5A 0A  0D 01000000 12 03 333333",  # f_bool_string wrong key type
            "5A 04  08 01 10 03",  # f_bool_string wrong value type
            "5A 0B  08 01 11 0300000000000000",  # f_bool_string wrong value type
            "5A 07  08 01 15 03000000",  # f_bool_string wrong value type
            "AA06 04  08 03 10 04",  # f_string_int32 wrong key type
            "AA06 0E  09 0300000000000000 333333 10 04",  # f_string_int32 wrong key type
            "AA06 0A  0D 03000000 333333 10 04",  # f_string_int32 wrong key type
            "AA06 0E  0A 03 333333 11 0300000000000000",  # f_string_int32 wrong value type
            "AA06 08  0A 03 333333 12 01 03",  # f_string_int32 wrong value type
            "AA06 0A  0A 03 333333 15 03000000",  # f_string_int32 wrong value type
            "B206 04  08 03 10 04",  # f_string_int64 wrong key type
            "B206 0E  09 0300000000000000 333333 10 04",  # f_string_int64 wrong key type
            "B206 0A  0D 03000000 333333 10 04",  # f_string_int64 wrong key type
            "B206 0E  0A 03 333333 11 0300000000000000",  # f_string_int64 wrong value type
            "B206 08  0A 03 333333 12 01 03",  # f_string_int64 wrong value type
            "B206 0A  0A 03 333333 15 03000000",  # f_string_int64 wrong value type
            "BA06 04  08 03 10 04",  # f_string_uint32 wrong key type
            "BA06 0E  09 0300000000000000 333333 10 04",  # f_string_uint32 wrong key type
            "BA06 0A  0D 03000000 333333 10 04",  # f_string_uint32 wrong key type
            "BA06 0E  0A 03 333333 11 0300000000000000",  # f_string_uint32 wrong value type
            "BA06 08  0A 03 333333 12 01 03",  # f_string_uint32 wrong value type
            "BA06 0A  0A 03 333333 15 03000000",  # f_string_uint32 wrong value type
            "C206 04  08 03 10 04",  # f_string_uint64 wrong key type
            "C206 0E  09 0300000000000000 333333 10 04",  # f_string_uint64 wrong key type
            "C206 0A  0D 03000000 333333 10 04",  # f_string_uint64 wrong key type
            "C206 0E  0A 03 333333 11 0300000000000000",  # f_string_uint64 wrong value type
            "C206 08  0A 03 333333 12 01 03",  # f_string_uint64 wrong value type
            "C206 0A  0A 03 333333 15 03000000",  # f_string_uint64 wrong value type
            "CA06 04  08 03 10 08",  # f_string_sint32 wrong key type
            "CA06 0E  09 0300000000000000 333333 10 08",  # f_string_sint32 wrong key type
            "CA06 0A  0D 03000000 333333 10 08",  # f_string_sint32 wrong key type
            "CA06 0E  0A 03 333333 11 0300000000000000",  # f_string_sint32 wrong value type
            "CA06 08  0A 03 333333 12 01 03",  # f_string_sint32 wrong value type
            "CA06 0A  0A 03 333333 15 03000000",  # f_string_sint32 wrong value type
            "D206 04  08 03 10 08",  # f_string_sint64 wrong key type
            "D206 0E  09 0300000000000000 333333 10 08",  # f_string_sint64 wrong key type
            "D206 0A  0D 03000000 333333 10 08",  # f_string_sint64 wrong key type
            "D206 0E  0A 03 333333 11 0300000000000000",  # f_string_sint64 wrong value type
            "D206 08  0A 03 333333 12 01 03",  # f_string_sint64 wrong value type
            "D206 0A  0A 03 333333 15 03000000",  # f_string_sint64 wrong value type
            "DA06 04  08 03 15 04000000",  # f_string_fixed32 wrong key type
            "DA06 0E  09 0300000000000000 333333 15 04000000",  # f_string_fixed32 wrong key type
            "DA06 0A  0D 03000000 333333 15 04000000",  # f_string_fixed32 wrong key type
            "DA06 07  0A 03 333333 10 04",  # f_string_fixed32 wrong value type
            "DA06 0E  0A 03 333333 11 040000000000000000",  # f_string_fixed32 wrong value type
            "DA06 08  0A 03 333333 12 01 04",  # f_string_fixed32 wrong value type
            "E206 04  08 03 11 0400000000000000",  # f_string_fixed64 wrong key type
            "E206 0E  09 0300000000000000 333333 11 0400000000000000",  # f_string_fixed64 wrong key type
            "E206 0A  0D 03000000 333333 11 0400000000000000",  # f_string_fixed64 wrong key type
            "E206 07  0A 03 333333 10 04",  # f_string_fixed64 wrong value type
            "E206 08  0A 03 333333 12 01 04",  # f_string_fixed64 wrong value type
            "E206 0A  0A 03 333333 15 04000000",  # f_string_fixed64 wrong value type
            "EA06 04  08 03 15 04000000",  # f_string_sfixed32 wrong key type
            "EA06 0E  09 0300000000000000 333333 15 04000000",  # f_string_sfixed32 wrong key type
            "EA06 0A  0D 03000000 333333 15 04000000",  # f_string_sfixed32 wrong key type
            "EA06 07  0A 03 333333 10 04",  # f_string_sfixed32 wrong value type
            "EA06 0E  0A 03 333333 11 040000000000000000",  # f_string_sfixed32 wrong value type
            "EA06 08  0A 03 333333 12 01 04",  # f_string_sfixed32 wrong value type
            "F206 04  08 03 11 0400000000000000",  # f_string_sfixed64 wrong key type
            "F206 0E  09 0300000000000000 333333 11 0400000000000000",  # f_string_sfixed64 wrong key type
            "F206 0A  0D 03000000 333333 11 0400000000000000",  # f_string_sfixed64 wrong key type
            "F206 07  0A 03 333333 10 04",  # f_string_sfixed64 wrong value type
            "F206 08  0A 03 333333 12 01 04",  # f_string_sfixed64 wrong value type
            "F206 0A  0A 03 333333 15 04000000",  # f_string_sfixed64 wrong value type
            "FA06 04  08 03 10 01",  # f_string_bool wrong key type
            "FA06 0E  09 0300000000000000 333333 10 01",  # f_string_bool wrong key type
            "FA06 0A  0D 03000000 333333 10 01",  # f_string_bool wrong key type
            "FA06 0E  0A 03 333333 11 0100000000000000",  # f_string_bool wrong value type
            "FA06 08  0A 03 333333 12 01 01",  # f_string_bool wrong value type
            "FA06 0A  0A 03 333333 15 01000000",  # f_string_bool wrong value type
            "8207 04  08 03 10 0A",  # f_string_enum1 wrong key type
            "8207 0E  09 0300000000000000 333333 10 0A",  # f_string_enum1 wrong key type
            "8207 0A  0D 03000000 333333 10 0A",  # f_string_enum1 wrong key type
            "8207 0E  0A 03 333333 11 0A00000000000000",  # f_string_enum1 wrong value type
            "8207 08  0A 03 333333 12 01 0A",  # f_string_enum1 wrong value type
            "8207 0A  0A 03 333333 15 0A000000",  # f_string_enum1 wrong value type
            "8A07 04  08 03 10 01",  # f_string_enum2 wrong key type
            "8A07 0E  09 0300000000000000 333333 10 01",  # f_string_enum2 wrong key type
            "8A07 0A  0D 03000000 333333 10 01",  # f_string_enum2 wrong key type
            "8A07 0E  0A 03 333333 11 0100000000000000",  # f_string_enum2 wrong value type
            "8A07 08  0A 03 333333 12 01 01",  # f_string_enum2 wrong value type
            "8A07 0A  0A 03 333333 15 01000000",  # f_string_enum2 wrong value type
            "9207 04  08 03 15 00008040",  # f_string_float wrong key type
            "9207 0E  09 0300000000000000 333333 15 00008040",  # f_string_float wrong key type
            "9207 0A  0D 03000000 333333 15 00008040",  # f_string_float wrong key type
            "9207 07  0A 03 333333 10 04",  # f_string_float wrong value type
            "9207 0E  0A 03 333333 11 0400000000000000",  # f_string_float wrong value type
            "9207 08  0A 03 333333 12 01 04",  # f_string_float wrong value type
            "9A07 04  08 03 11 0000000000001040",  # f_string_double wrong key type
            "9A07 0E  09 0300000000000000 333333 11 0000000000001040",  # f_string_double wrong key type
            "9A07 0A  0D 03000000 333333 11 0000000000001040",  # f_string_double wrong key type
            "9A07 07  0A 03 333333 10 04",  # f_string_double wrong value type
            "9A07 08  0A 03 333333 12 01 04",  # f_string_double wrong value type
            "9A07 0A  0A 03 333333 15 00008040",  # f_string_double wrong value type
            "A207 04  08 03 12 04 34343434",  # f_string_bytes wrong key type
            "A207 0E  09 0300000000000000 333333 12 04 34343434",  # f_string_bytes wrong key type
            "A207 0A  0D 03000000 333333 12 04 34343434",  # f_string_bytes wrong key type
            "A207 07  0A 03 333333 10 04",  # f_string_bytes wrong value type
            "A207 0E  0A 03 333333 11 0400000000000000",  # f_string_bytes wrong value type
            "A207 0A  0A 03 333333 15 04000000",  # f_string_bytes wrong value type
            "B207 04  08 03 12 02 10 04",  # f_string_message wrong key type
            "B207 0E  09 0300000000000000 333333 12 02 10 04",  # f_string_message wrong key type
            "B207 0A  0D 03000000 333333 12 02 10 04",  # f_string_message wrong key type
            "B207 07  0A 03 333333 10 04",  # f_string_message wrong value type
            "B207 0E  0A 03 333333 11 0400000000000000",  # f_string_message wrong value type
            "B207 0A  0A 03 333333 15 04000000",  # f_string_message wrong value type
        ):
            assert_parsing_fails(pbcc.TestMaps, bytes.fromhex(test_case), "Incorrect type")


@test_case
def test_deserialize_garbage() -> None:
    # We'll use variations of the following two messages to test things:
    # pbcc.TestOneofs(
    #   f_submessage=pbcc.TestPrimitives(          # 2A 0E
    #     f_bytes=b'wtf hax',                      #   8201 07 77746620686178
    #     f_sint32=5,                              #   28 0A
    #     f_enum1=pbcc.TestEnum1.TEST_E1_VALUE2))  #   60 05
    # pbcc.TestMaps(
    #   f_string_fixed64={
    #     'omg': 8004,                      # E206 0E [0A 03 6F6D67; 11 441F000000000000]
    #     'hax': 348,                       # E206 0E [0A 03 686178; 11 5C01000000000000]
    #     'lol': 103})                      # E206 0E [0A 03 6C6F6C; 11 6700000000000000]
    assert_parsing_fails(pbcc.TestOneofs, bytes.fromhex("2A0E82010777746620686178280A6006"))  # Invalid enum value
    assert_parsing_fails(
        pbcc.TestOneofs, bytes.fromhex("2A0E82010777746620686178280A60")
    )  # Incomplete field (enum tag present but value missing)
    assert_parsing_fails(
        pbcc.TestOneofs, bytes.fromhex("2A0E82010777746620686178280A67000000")
    )  # Invalid tag (7 isn't a valid wire type)
    assert_parsing_fails(pbcc.TestOneofs, bytes.fromhex("FFFFFFFFFFFFFFFFFFFFFFFFFFFF05"))  # Varint value too large
    assert_parsing_fails(
        pbcc.TestMaps, bytes.fromhex("E2060E0A036F6D6711441F0000000000")
    )  # Field too short (0x0D bytes, not 0x0E)
    assert_parsing_fails(
        pbcc.TestPrimitives, bytes.fromhex("1208")
    )  # Wrong field type (0x12 => field 2, LENGTH; should be VARINT)


@test_case
def test_retain_unknown_fields() -> None:
    primitives_m = pbcc.TestPrimitives(f_float=2.0, f_double=4.0, f_uint64=64)
    data = primitives_m.as_proto_data()
    print(f"... data={data!r}")

    floats_m = pbcc.TestFloatPrimitivesOnly.from_proto_data(data)
    assert floats_m.has_unknown_fields()
    assert floats_m.f_float == 2.0
    assert floats_m.f_double == 4.0
    data2 = floats_m.as_proto_data()
    print(f"... data={data2!r}")
    primitives_m2 = pbcc.TestPrimitives.from_proto_data(data2)
    assert not primitives_m2.has_unknown_fields()
    assert primitives_m2.f_uint64 == 64
    assert primitives_m2.f_float == 2.0
    assert primitives_m2.f_double == 4.0

    # Same as above, but we delete unknown fields after parsing
    # TODO: Also test passing retain_unknown_fields=False when parsing
    floats_m = pbcc.TestFloatPrimitivesOnly.from_proto_data(data)
    assert floats_m.has_unknown_fields()
    floats_m.delete_unknown_fields()
    assert not floats_m.has_unknown_fields()
    assert floats_m.f_float == 2.0
    assert floats_m.f_double == 4.0
    data2 = floats_m.as_proto_data()
    print(f"... data={data2!r}")
    primitives_m2 = pbcc.TestPrimitives.from_proto_data(data2)
    assert not primitives_m2.has_unknown_fields()
    assert primitives_m2.f_uint64 == 0
    assert primitives_m2.f_float == 2.0
    assert primitives_m2.f_double == 4.0


@test_case
def test_field_ordering() -> None:
    # Create message with fields intentionally defined out of order in proto
    msg = pbcc.TestFieldOrdering(
        last_field="last",
        first_field=1,
        middle_field="middle",
        second_field=2,
        fourth_field="fourth",
    )

    # The repr should show fields in order by field number regardless of definition order
    expected_repr = "test_pbcc.test.TestFieldOrdering(first_field=1, second_field=2, middle_field='middle', fourth_field='fourth', last_field='last')"
    assert repr(msg) == expected_repr, repr(msg)

    # Test equality with same values but different field order
    msg2 = pbcc.TestFieldOrdering(
        first_field=1,
        second_field=2,
        middle_field="middle",
        fourth_field="fourth",
        last_field="last",
    )

    # They should be equal regardless of initialization order
    assert msg == msg2, "Messages with same values in different order should be equal"

    # Test serialization is consistent
    assert msg.as_proto_data() == msg2.as_proto_data(), "Proto serialization should be consistent"

    # Test deserialization preserves equality
    msg3 = pbcc.TestFieldOrdering.from_proto_data(msg.as_proto_data())
    assert msg3 == msg, "Deserialized message should equal original"


@test_case
def test_long_field_repr() -> None:
    # Create a long string and long bytes
    long_string = "This is a very long string that should be fully repr'd in the output. " * 10
    long_bytes = b"This is a very long bytes that should be truncated in the output. " * 10

    # Create a message with both fields
    msg = pbcc.TestPrimitives(f_string=long_string, f_bytes=long_bytes)

    # Get the repr
    msg_repr = repr(msg)

    # The string should be fully repr'd
    assert long_string in msg_repr, "Long string should be fully repr'd"

    # The bytes should be truncated
    assert len(long_bytes) > 100  # Sanity check that bytes are long
    assert len(msg_repr.split("f_bytes=")[1].split(",")[0]) < 100, "Long bytes should be truncated in repr"

    # Verify the actual values are preserved
    assert msg.f_string == long_string
    assert msg.f_bytes == long_bytes


def run_all_tests() -> int:
    num_failures: int = 0
    for name, fn in ALL_TEST_CASES:
        try:
            fn()
            print(f"... {fn.__name__} PASS")
        except Exception:
            print(f"... {fn.__name__} FAIL")
            traceback.print_exc()
            num_failures += 1

    if num_failures == 0:
        print(f"... {len(ALL_TEST_CASES)} TESTS PASSED")
    else:
        print(f"... {num_failures}/{len(ALL_TEST_CASES)} TESTS FAILED")

    return num_failures


if __name__ == "__main__":
    sys.exit(run_all_tests() != 0)
