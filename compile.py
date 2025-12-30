from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import enum
import importlib
import logging
import os
import re
import sys
import tempfile
from typing import Any, Awaitable, Iterable, Literal, Sequence, cast

from google.protobuf.descriptor import Descriptor as MessageDescriptor
from google.protobuf.descriptor import (
    EnumDescriptor,
    EnumValueDescriptor,
    FieldDescriptor,
    FileDescriptor,
    OneofDescriptor,
)

from async_utils import check_call_async, check_output_async


class DataType(enum.Enum):
    FLOAT = enum.auto()
    DOUBLE = enum.auto()
    INT32 = enum.auto()
    UINT32 = enum.auto()
    SINT32 = enum.auto()
    INT64 = enum.auto()
    UINT64 = enum.auto()
    SINT64 = enum.auto()
    FIXED32 = enum.auto()
    SFIXED32 = enum.auto()
    FIXED64 = enum.auto()
    SFIXED64 = enum.auto()
    BOOL = enum.auto()
    ENUM = enum.auto()
    STRING = enum.auto()
    BYTES = enum.auto()
    MAP = enum.auto()
    MESSAGE = enum.auto()


DATA_TYPE_FOR_DESCRIPTOR_TYPE: dict[int, DataType] = {
    FieldDescriptor.TYPE_FLOAT: DataType.FLOAT,
    FieldDescriptor.TYPE_DOUBLE: DataType.DOUBLE,
    FieldDescriptor.TYPE_INT32: DataType.INT32,
    FieldDescriptor.TYPE_UINT32: DataType.UINT32,
    FieldDescriptor.TYPE_SINT32: DataType.SINT32,
    FieldDescriptor.TYPE_INT64: DataType.INT64,
    FieldDescriptor.TYPE_UINT64: DataType.UINT64,
    FieldDescriptor.TYPE_SINT64: DataType.SINT64,
    FieldDescriptor.TYPE_FIXED32: DataType.FIXED32,
    FieldDescriptor.TYPE_SFIXED32: DataType.SFIXED32,
    FieldDescriptor.TYPE_FIXED64: DataType.FIXED64,
    FieldDescriptor.TYPE_SFIXED64: DataType.SFIXED64,
    FieldDescriptor.TYPE_BOOL: DataType.BOOL,
    FieldDescriptor.TYPE_ENUM: DataType.ENUM,
    FieldDescriptor.TYPE_STRING: DataType.STRING,
    FieldDescriptor.TYPE_BYTES: DataType.BYTES,
    FieldDescriptor.TYPE_MESSAGE: DataType.MESSAGE,
    # FieldDescriptor.TYPE_GROUP not supported
}

CC_DEFAULT_VALUE_CONSTRUCTOR_FOR_PRIMITIVE_DATA_TYPE: dict[DataType, str] = {
    DataType.FLOAT: "create_py_float_zero()",
    DataType.DOUBLE: "create_py_float_zero()",
    DataType.INT32: "create_py_int_zero()",
    DataType.UINT32: "create_py_int_zero()",
    DataType.SINT32: "create_py_int_zero()",
    DataType.INT64: "create_py_int_zero()",
    DataType.UINT64: "create_py_int_zero()",
    DataType.SINT64: "create_py_int_zero()",
    DataType.FIXED32: "create_py_int_zero()",
    DataType.SFIXED32: "create_py_int_zero()",
    DataType.FIXED64: "create_py_int_zero()",
    DataType.SFIXED64: "create_py_int_zero()",
    DataType.BOOL: "create_py_false()",
    DataType.ENUM: "__INVALID__",  # Special-cased in default_value_constructor_for_field_group
    DataType.STRING: "create_py_empty_str()",
    DataType.BYTES: "create_py_empty_bytes()",
    DataType.MAP: "create_py_empty_dict()",
    DataType.MESSAGE: "__INVALID__",  # Special-cased in default_value_constructor_for_field_group
}

PY_TYPE_FOR_PRIMITIVE_DATA_TYPE: dict[DataType, str] = {
    DataType.FLOAT: "float",
    DataType.DOUBLE: "float",
    DataType.INT32: "int",
    DataType.UINT32: "int",
    DataType.SINT32: "int",
    DataType.INT64: "int",
    DataType.UINT64: "int",
    DataType.SINT64: "int",
    DataType.FIXED32: "int",
    DataType.SFIXED32: "int",
    DataType.FIXED64: "int",
    DataType.SFIXED64: "int",
    DataType.BOOL: "bool",
    DataType.ENUM: "__INVALID__",  # Special-cased in py_type_for_field_group
    DataType.STRING: "str",
    DataType.BYTES: "bytes",
    DataType.MAP: "__INVALID__",  # Special-cased in py_type_for_field_group
    DataType.MESSAGE: "__INVALID__",  # Special-cased in py_type_for_field_group
}


@dataclasses.dataclass(kw_only=True)
class EnumInfo:
    module_name: str
    name: str
    members: dict[str, int]

    def pyi_source_lines(self, indent_level: int = 0) -> list[str]:
        indent_str = "    " * indent_level
        ret = [f"{indent_str}class {cc_name_for_python_name(self.name)}(IntEnum):"]
        for name, value in sorted(self.members.items(), key=lambda it: it[1]):
            ret.append(f"{indent_str}    {name} = {value}")
        return ret


@dataclasses.dataclass(kw_only=True)
class FieldInfo:
    py_name: str
    is_optional: bool
    is_repeated: bool
    data_type: DataType
    enum: EnumInfo | None
    submessage: MessageInfo | None
    field_num: int

    def type_key(self) -> str:
        if self.data_type == DataType.ENUM:
            assert self.enum is not None
            return repr(("ENUM", self.enum.module_name, self.enum.name))
        elif self.data_type == DataType.MAP:
            assert self.submessage is not None
            assert self.submessage.map_types is not None
            return repr(
                (
                    "MAP",
                    self.submessage.map_types[0].type_key(),
                    self.submessage.map_types[1].type_key(),
                )
            )
        elif self.data_type == DataType.MESSAGE:
            assert self.submessage is not None
            return repr(("MESSAGE", self.submessage.module_name, self.submessage.name))
        else:
            return PY_TYPE_FOR_PRIMITIVE_DATA_TYPE[self.data_type]


def cc_name_for_enum_or_message_info(ent: EnumInfo | MessageInfo) -> str:
    return f"{cc_name_for_python_name(ent.module_name)}__{cc_name_for_python_name(ent.name)}"


def field_group_is_repeated(fields: Sequence[FieldInfo]) -> bool:
    # All fields in the oneof must be repeated, or none must be
    is_repeated_sum = sum(f.is_repeated for f in fields)
    if is_repeated_sum == len(fields):
        return True
    else:
        assert is_repeated_sum == 0, "Some, but not all, fields in a oneof are repeated"
        return False


def default_value_constructor_for_field_group(fields: Sequence[FieldInfo]) -> str:
    # If any field in the oneof is optional, the default value is None
    if any(f.is_optional for f in fields):
        return "create_py_none()"

    # All fields in the oneof must be repeated, or none must be. If all are
    # repeated, then the default value is an empty list or dict
    if field_group_is_repeated(fields):
        if len(fields) == 1 and fields[0].data_type == DataType.MAP:
            return "create_py_empty_dict()"
        else:
            return "create_py_empty_list()"

    # If neither of the above, then the default value is the first type in the
    # oneof
    first_field = fields[0]
    if first_field.data_type == DataType.ENUM:
        assert first_field.enum is not None
        return f"{cc_name_for_enum_or_message_info(first_field.enum)}_enum_ref.py_member_for_value(0).new_ref()"
    if first_field.data_type == DataType.MESSAGE:
        assert first_field.submessage is not None
        cc_cls_name = cc_name_for_enum_or_message_info(first_field.submessage)
        return f"{cc_cls_name}::py_new(&{cc_cls_name}::py_type, nullptr, nullptr)"
    return CC_DEFAULT_VALUE_CONSTRUCTOR_FOR_PRIMITIVE_DATA_TYPE[first_field.data_type]


def py_type_for_field_group(fields: Sequence[FieldInfo]) -> str:
    types = []
    for field in fields:
        field_type: str
        if field.data_type == DataType.ENUM:
            assert field.enum is not None
            field_type = f"{field.enum.module_name}.{cc_name_for_python_name(field.enum.name)}"
        elif field.data_type == DataType.MESSAGE:
            assert field.submessage is not None
            field_type = f"{field.submessage.module_name}.{cc_name_for_python_name(field.submessage.name)}"
        elif field.data_type == DataType.MAP:
            assert field.submessage is not None
            assert field.submessage.map_types is not None
            key_type = py_type_for_field_group([field.submessage.map_types[0]])
            value_type = py_type_for_field_group([field.submessage.map_types[1]])
            field_type = f"dict[{key_type}, {value_type}]"
        else:
            field_type = PY_TYPE_FOR_PRIMITIVE_DATA_TYPE[field.data_type]

        types.append(f"list[{field_type}]" if (field.is_repeated and field.data_type != DataType.MAP) else field_type)

    if any(f.is_optional for f in fields):
        types.append("None")

    return " | ".join(types)


def full_name_for_descriptor(desc: EnumDescriptor | MessageDescriptor) -> str:
    name: str = desc.name
    containing_message_desc: MessageDescriptor = desc.containing_type
    while containing_message_desc:
        name = f"{containing_message_desc.name}.{name}"
        containing_message_desc = containing_message_desc.containing_type
    return name


def name_for_module_path(filename: str) -> str:
    return os.path.splitext(os.path.basename(filename))[0]


def namespaced_name_for_descriptor(desc: EnumDescriptor | MessageDescriptor) -> str:
    assert isinstance(desc.file, FileDescriptor)
    return f"{name_for_module_path(desc.file.name)}.{full_name_for_descriptor(desc)}"


@dataclasses.dataclass(kw_only=True)
class MessageInfo:
    module_name: str
    name: str
    field_for_number: dict[int, FieldInfo] = dataclasses.field(default_factory=dict)
    field_groups: dict[str, list[FieldInfo]] = dataclasses.field(default_factory=lambda: collections.defaultdict(list))
    map_types: tuple[FieldInfo, FieldInfo] | None  # If not None, this message is a map entry message

    def pyi_source_lines(self, indent_level: int = 0) -> list[str]:
        indent_str = "    " * indent_level
        cc_cls_name = cc_name_for_python_name(self.name)
        namespaced_name = f"{self.module_name}.{cc_cls_name}"

        ret: list[str] = []

        def add_line(line: str) -> None:
            ret.append((indent_str + line) if len(line) > 0 else "")

        if len(self.field_groups) == 1:
            slots_strs = f"{repr(next(iter(self.field_groups.keys())))},"
        else:
            slots_strs = ", ".join(repr(name) for name in self.field_groups.keys())
        add_line(f"class {cc_cls_name}:")
        add_line(f"    __slots__ = ({slots_strs})")

        init_args: list[str] = ["self", "*"]
        for name, field_group in sorted(self.field_groups.items(), key=lambda it: min(f.field_num for f in it[1])):
            py_type = py_type_for_field_group(field_group)
            field_nums_str = ", ".join(str(f.field_num) for f in field_group)
            add_line(f"    {name}: {py_type}  # {field_nums_str}")
            init_args.append(f"{name}: {py_type} = ...")

        add_line("")
        init_args_str = ", ".join(init_args)
        add_line(f"    def __init__({init_args_str}): ...")
        add_line("")
        add_line("    @staticmethod")
        add_line(
            f"    def from_proto_data(data: bytes, retain_unknown_fields: bool = True, ignore_incorrect_types: bool = False) -> {namespaced_name}: ..."
        )
        add_line(
            "    def parse_proto_into_this(self, data: bytes, retain_unknown_fields: bool = True, ignore_incorrect_types: bool = False) -> None: ..."
        )
        add_line("")
        add_line("    def as_proto_data(self) -> bytes: ...")
        add_line("    def as_dict(self) -> dict[str, Any]: ...")
        add_line("")
        add_line(f"    def proto_copy({init_args_str}) -> {namespaced_name}: ...")
        add_line("")
        add_line("    def has_unknown_fields(self) -> bool: ...")
        add_line("    def delete_unknown_fields(self) -> None: ...")
        add_line("    def get_unknown_fields(self) -> dict[int, bytes]: ...")
        return ret


def cc_name_for_python_name(name: str) -> str:
    return name.replace(".", "_")


@dataclasses.dataclass(kw_only=True)
class ModuleInfo:
    name: str
    enums: dict[str, EnumInfo] = dataclasses.field(default_factory=dict)
    messages: dict[str, MessageInfo] = dataclasses.field(default_factory=dict)

    _in_progress: bool = False

    def pyi_source_lines(self, indent_level: int = 0) -> list[str]:
        ret: list[str] = []
        for _, proto_enum in sorted(self.enums.items()):
            ret += proto_enum.pyi_source_lines(indent_level=indent_level)
            ret.append("")
        for _, message in sorted(self.messages.items()):
            if message.map_types is None:
                ret += message.pyi_source_lines(indent_level=indent_level)
                ret.append("")
        return ret


@dataclasses.dataclass(kw_only=True)
class ModuleCollection:
    modules: dict[str, ModuleInfo]
    global_aliases: dict[str, MessageInfo | EnumInfo | None] = dataclasses.field(default_factory=dict)

    def compute_global_aliases(self) -> None:
        print("Populating global aliases")
        for mod_info in self.modules.values():
            for msg_info in mod_info.messages.values():
                if msg_info.map_types is not None:
                    continue
                if msg_info.name in self.global_aliases:
                    self.global_aliases[msg_info.name] = None
                else:
                    self.global_aliases[msg_info.name] = msg_info
            for enum_info in mod_info.enums.values():
                if enum_info.name in self.global_aliases:
                    self.global_aliases[enum_info.name] = None
                else:
                    self.global_aliases[enum_info.name] = enum_info

        for name, ent_info in self.global_aliases.items():
            if ent_info is None:
                print(
                    f"Warning: multiple entities named {name} exist in different modules; global alias will be suppressed"
                )

    def _collect_descriptor(self, mod_info: ModuleInfo, ent_desc: EnumDescriptor | MessageDescriptor) -> None:
        # If this descriptor is defined in a different module, parse that module, or raise if it's currently in
        # progress (which would indicate an import cycle)
        assert isinstance(ent_desc.file, FileDescriptor)
        ent_mod_name = name_for_module_path(ent_desc.file.name)
        ent_name = full_name_for_descriptor(ent_desc)
        ent_namespaced_name = f"{ent_mod_name}.{ent_name}"

        if ent_mod_name != mod_info.name:
            ent_mod = self.modules.get(ent_mod_name, None)
            if ent_mod is None:
                ent_mod = self.add_file(ent_desc.file)
            else:
                assert not ent_mod._in_progress, f"Import cycle detected involving {ent_mod.name}"
            assert ent_name in ent_mod.enums or ent_name in ent_mod.messages

        elif isinstance(ent_desc, EnumDescriptor):
            if ent_name in mod_info.enums:
                return
            enum_info = EnumInfo(
                module_name=ent_mod_name,
                name=ent_name,
                members={
                    k: cast(EnumValueDescriptor, v).number for k, v in cast(dict, ent_desc.values_by_name).items()
                },
            )
            mod_info.enums[ent_name] = enum_info
            print(f"... Adding enum {ent_namespaced_name}")

        else:
            if ent_name in mod_info.messages:
                return

            assert len(ent_desc.enum_types) == 0, (
                f"Enums defined within message classes are not supported (message: {ent_namespaced_name})"
            )

            message = MessageInfo(module_name=ent_mod_name, name=ent_name, map_types=None)
            mod_info.messages[ent_name] = message
            print(f"... Adding message {ent_namespaced_name}")

            # Collect all sub-entities
            for enum_desc in ent_desc.enum_types:
                self._collect_descriptor(mod_info, enum_desc)
            for submsg_desc in ent_desc.nested_types:
                self._collect_descriptor(mod_info, submsg_desc)
            for fld_desc in cast(list[FieldDescriptor], ent_desc.fields):
                if fld_desc.enum_type is not None:
                    self._collect_descriptor(mod_info, fld_desc.enum_type)
                if fld_desc.message_type is not None:
                    self._collect_descriptor(mod_info, fld_desc.message_type)

            for fld_desc in cast(list[FieldDescriptor], ent_desc.fields):
                oneof: OneofDescriptor | None = fld_desc.containing_oneof
                # Optional fields are apparently implemented as a oneof with a
                # single field inside, and using the field's name prefixed with
                # an underscore
                is_optional = (oneof is not None) and (len(oneof.fields) == 1) and (oneof.name == f"_{fld_desc.name}")
                if is_optional:
                    oneof = None
                py_name = oneof.name if oneof is not None else fld_desc.name
                data_type = DATA_TYPE_FOR_DESCRIPTOR_TYPE[fld_desc.type]
                if fld_desc.enum_type is not None:
                    assert isinstance(fld_desc.enum_type, EnumDescriptor)
                    assert isinstance(fld_desc.enum_type.file, FileDescriptor)
                    fld_enum_mod_name = name_for_module_path(fld_desc.enum_type.file.name)
                    fld_enum_name = full_name_for_descriptor(fld_desc.enum_type)
                    fld_enum = self.modules[fld_enum_mod_name].enums[fld_enum_name]
                else:
                    fld_enum = None
                if fld_desc.message_type is not None:
                    assert isinstance(fld_desc.message_type, MessageDescriptor)
                    assert isinstance(fld_desc.message_type.file, FileDescriptor)
                    fld_msg_mod_name = name_for_module_path(fld_desc.message_type.file.name)
                    fld_msg_name = full_name_for_descriptor(fld_desc.message_type)
                    fld_msg = self.modules[fld_msg_mod_name].messages[fld_msg_name]
                    if fld_msg.map_types is not None:
                        data_type = DataType.MAP
                else:
                    fld_msg = None
                fi = FieldInfo(
                    py_name=py_name,
                    is_optional=is_optional,
                    is_repeated=fld_desc.is_repeated,
                    data_type=data_type,
                    enum=fld_enum,
                    submessage=fld_msg,
                    field_num=fld_desc.number,
                )
                message.field_for_number[fi.field_num] = fi
                fg = message.field_groups[py_name]
                type_key = fi.type_key()
                for other_fi in fg:
                    assert other_fi.type_key() != type_key, (
                        f"All fields in oneofs must have distinct Python types; fields in group {ent_name}.{fi.py_name} do not"
                    )
                fg.append(fi)

            # Check if this is a map message and populate its type info if so
            if (
                message.name.endswith("Entry")
                and (sorted(message.field_for_number.keys()) == [1, 2])
                and (sorted(message.field_groups.keys()) == ["key", "value"])
            ):
                message.map_types = (
                    message.field_for_number[1],
                    message.field_for_number[2],
                )

    def add_file(self, mod_desc: FileDescriptor) -> ModuleInfo:
        mod_name = name_for_module_path(mod_desc.name)
        existing_mod_info = self.modules.get(mod_name, None)
        if existing_mod_info is not None:
            assert not existing_mod_info._in_progress, f"Import cycle detected involving {mod_name}"
            return existing_mod_info

        mod_info = ModuleInfo(name=mod_name, _in_progress=True)
        self.modules[mod_info.name] = mod_info
        print(f"... Adding module {mod_info.name}")

        for msg_desc in cast(dict[str, MessageDescriptor], mod_desc.message_types_by_name).values():
            self._collect_descriptor(mod_info, msg_desc)
        for enum_desc in cast(dict[str, EnumDescriptor], mod_desc.enum_types_by_name).values():
            self._collect_descriptor(mod_info, enum_desc)

        mod_info._in_progress = False
        return mod_info

    def cc_source(self, so_module_name: str, add_line_directives: bool = True) -> str:
        template_path = os.path.relpath(os.path.join(os.path.dirname(__file__), "pymodule.in.cc"))
        with open(template_path, "rt") as f:
            template_lines = [line.rstrip() for line in f.readlines()]

        re_comment_tag = re.compile(r"\s*// (?P<tag>__COMPILER__[A-Za-z0-9_]+?__)$")
        re_inline_tag = re.compile(r"__COMPILER__[A-Za-z0-9_]+?__")

        def get_block_end_line(start_line_num: int) -> int:
            line_num = start_line_num
            block_stack: list[Literal["FOREACH", "IF"]] = []
            while line_num == start_line_num or len(block_stack) > 0:
                comment_tag_m = re_comment_tag.match(template_lines[line_num])
                line_num += 1
                if comment_tag_m is None:
                    continue
                tag = comment_tag_m.group("tag")
                if tag == "__COMPILER__END_IF__":
                    assert len(block_stack) > 0 and block_stack.pop() == "IF", (
                        f"Unterminated IF block ending at line {start_line_num + 1}"
                    )
                elif tag == "__COMPILER__END_FOREACH__":
                    assert len(block_stack) > 0 and block_stack.pop() == "FOREACH", (
                        f"Unterminated FOREACH block ending at line {start_line_num + 1}"
                    )
                elif tag.startswith("__COMPILER__IF_"):
                    block_stack.append("IF")
                elif tag.startswith("__COMPILER__FOREACH_"):
                    block_stack.append("FOREACH")
                else:
                    assert False, f"Invalid comment tag at line {start_line_num + 1}: {tag}"
            return line_num

        result_lines: list[str] = []

        def add_line_directive(line_num: int, annotations: Sequence[str]) -> None:
            if add_line_directives:
                if annotations:
                    annotated_filename = template_path + "(" + ",".join(annotations) + ")"
                else:
                    annotated_filename = template_path
                result_lines.append(f'#line {line_num + 1} "{annotated_filename}"')

        def replace_template_scope(
            start_line_num: int,
            end_line_num: int,
            env: dict[str, str],
            annotations: Sequence[str] = (),
        ) -> None:
            add_line_directive(start_line_num, annotations)
            line_num = start_line_num
            try:
                while line_num < end_line_num:
                    template_line = template_lines[line_num]
                    comment_tag_m = re_comment_tag.match(template_line)
                    if comment_tag_m is not None:
                        tag = comment_tag_m.group("tag")
                        block_end_line = get_block_end_line(line_num)
                        match tag:
                            case "__COMPILER__FOREACH_MODULE__":
                                for mod_name in sorted(self.modules.keys()):
                                    sub_env = {
                                        **env,
                                        "__COMPILER__MODULE_NAME__": mod_name,
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"mod={mod_name}"),
                                    )
                            case "__COMPILER__FOREACH_ENUM__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                # This ordering is important! We need root objects to appear before their children, so
                                # e.g. `Message1` appears before `Message1.Submessage1`.
                                for _, enum in sorted(mod.enums.items()):
                                    sub_env = {
                                        **env,
                                        "__COMPILER__ENUM_PYTHON_NAME__": enum.name,
                                        "__COMPILER__ENUM_PYTHON_NAME_ESCAPED__": cc_name_for_python_name(enum.name),
                                        "__COMPILER__ENUM_CC_NAME__": cc_name_for_enum_or_message_info(enum),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"enum={enum.name}"),
                                    )
                            case "__COMPILER__FOREACH_GLOBAL_ENUM_ALIAS__":
                                for _, ent in sorted(self.global_aliases.items()):
                                    if ent is None or not isinstance(ent, EnumInfo):
                                        continue
                                    sub_env = {
                                        **env,
                                        "__COMPILER__ENUM_PYTHON_NAME__": ent.name,
                                        "__COMPILER__ENUM_PYTHON_NAME_ESCAPED__": cc_name_for_python_name(ent.name),
                                        "__COMPILER__ENUM_CC_NAME__": cc_name_for_enum_or_message_info(ent),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"enum={ent.name}"),
                                    )
                            case "__COMPILER__FOREACH_ENUM_MEMBER__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                enum = mod.enums[env["__COMPILER__ENUM_PYTHON_NAME__"]]
                                for member_name, member_value in sorted(enum.members.items()):
                                    sub_env = {
                                        **env,
                                        "__COMPILER__ENUM_MEMBER_NAME__": member_name,
                                        "__COMPILER__ENUM_MEMBER_VALUE__": str(member_value),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"mem={member_name}"),
                                    )
                            case "__COMPILER__FOREACH_MESSAGE__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                # This ordering is important! We need root objects to appear before their children, so e.g.
                                # `Message1` appears before `Message1.Submessage1`.
                                for _, message in sorted(mod.messages.items()):
                                    if message.map_types is not None:
                                        continue
                                    sub_env = {
                                        **env,
                                        "__COMPILER__MESSAGE_PYTHON_NAME__": message.name,
                                        "__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__": cc_name_for_python_name(
                                            message.name
                                        ),
                                        "__COMPILER__MESSAGE_CC_NAME__": cc_name_for_enum_or_message_info(message),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"msg={message.name}"),
                                    )
                            case "__COMPILER__FOREACH_GLOBAL_MESSAGE_ALIAS__":
                                for _, ent in sorted(self.global_aliases.items()):
                                    if ent is None or not isinstance(ent, MessageInfo) or ent.map_types is not None:
                                        continue
                                    sub_env = {
                                        **env,
                                        "__COMPILER__MESSAGE_PYTHON_NAME__": ent.name,
                                        "__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__": cc_name_for_python_name(ent.name),
                                        "__COMPILER__MESSAGE_CC_NAME__": cc_name_for_enum_or_message_info(ent),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"msg={ent.name}"),
                                    )
                            case "__COMPILER__FOREACH_MESSAGE_FIELD_GROUP__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                # Sort field groups by minimum field number in each group for consistent ordering
                                sorted_groups = sorted(
                                    message.field_groups.items(),
                                    key=lambda item: min(f.field_num for f in item[1]),
                                )
                                for group_name, fields in sorted_groups:
                                    sub_env = {
                                        **env,
                                        "__COMPILER__MESSAGE_FIELD_GROUP_NAME__": group_name,
                                        "__COMPILER__MESSAGE_FIELD_GROUP_DEFAULT_VALUE_CONSTRUCTOR__": default_value_constructor_for_field_group(
                                            fields
                                        ),
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"grp={group_name}"),
                                    )
                            case "__COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                group = message.field_groups[env["__COMPILER__MESSAGE_FIELD_GROUP_NAME__"]]
                                for field in sorted(group, key=lambda f: f.field_num):
                                    enum_ref = "nullptr"
                                    parse_fn = "nullptr"
                                    serialize_fn = "nullptr"
                                    submessage_type_obj = "nullptr"
                                    # These two should only be used within a __COMPILER__IF_MESSAGE_FIELD_TYPE_MAP__,
                                    # so we intentionally use values that won't compile
                                    key_type = "__INVALID__"
                                    value_type = "__INVALID__"
                                    value_enum_ref = "nullptr"
                                    value_parse_fn = "nullptr"
                                    value_serialize_fn = "nullptr"
                                    value_submessage_type_obj = "nullptr"

                                    if field.enum is not None:
                                        enum_ref = f"&{cc_name_for_enum_or_message_info(field.enum)}_enum_ref"
                                    if field.submessage is not None:
                                        submsg_cc_name = cc_name_for_enum_or_message_info(field.submessage)
                                        parse_fn = (
                                            f"reinterpret_cast<ParseMessageFn>({submsg_cc_name}::from_proto_data)"
                                        )
                                        serialize_fn = f"{submsg_cc_name}::as_proto_data"
                                        submessage_type_obj = f"&{submsg_cc_name}::py_type"
                                        if field.submessage.map_types is not None:
                                            key_field, value_field = field.submessage.map_types
                                            key_type = key_field.data_type.name
                                            value_type = value_field.data_type.name
                                            value_enum_ref = (
                                                f"&{cc_name_for_enum_or_message_info(value_field.enum)}_enum_ref"
                                                if value_field.enum is not None
                                                else "nullptr"
                                            )
                                            if value_field.submessage is not None:
                                                value_submsg_name = cc_name_for_enum_or_message_info(
                                                    value_field.submessage
                                                )
                                                value_parse_fn = f"reinterpret_cast<ParseMessageFn>({value_submsg_name}::from_proto_data)"
                                                value_serialize_fn = f"{value_submsg_name}::as_proto_data"
                                                value_submessage_type_obj = f"&{value_submsg_name}::py_type"

                                    sub_env = {
                                        **env,
                                        "__COMPILER__MESSAGE_FIELD_IS_OPTIONAL__": (
                                            "true" if field.is_optional else "false"
                                        ),
                                        "__COMPILER__MESSAGE_FIELD_NUMBER__": str(field.field_num),
                                        "__COMPILER__MESSAGE_FIELD_DATA_TYPE__": field.data_type.name,
                                        "__COMPILER__MESSAGE_FIELD_ENUM_REF__": enum_ref,
                                        "__COMPILER__MESSAGE_FIELD_SUBMESSAGE_TYPE_OBJ__": submessage_type_obj,
                                        "__COMPILER__MESSAGE_FIELD_MESSAGE_PARSE_FN__": parse_fn,
                                        "__COMPILER__MESSAGE_FIELD_MESSAGE_SERIALIZE_FN__": serialize_fn,
                                        "__COMPILER__MESSAGE_FIELD_KEY_TYPE__": key_type,
                                        "__COMPILER__MESSAGE_FIELD_VALUE_TYPE__": value_type,
                                        "__COMPILER__MESSAGE_FIELD_VALUE_ENUM_REF__": value_enum_ref,
                                        "__COMPILER__MESSAGE_FIELD_VALUE_MESSAGE_PARSE_FN__": value_parse_fn,
                                        "__COMPILER__MESSAGE_FIELD_VALUE_MESSAGE_SERIALIZE_FN__": value_serialize_fn,
                                        "__COMPILER__MESSAGE_FIELD_VALUE_SUBMESSAGE_TYPE_OBJ__": value_submessage_type_obj,
                                    }
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        sub_env,
                                        (*annotations, f"fld={field.field_num}"),
                                    )

                            case "__COMPILER__IF_MESSAGE_FIELD_GROUP_IS_NOT_ONEOF__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                group = message.field_groups[env["__COMPILER__MESSAGE_FIELD_GROUP_NAME__"]]
                                assert len(group) > 0
                                if len(group) == 1:
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        env,
                                        (*annotations, "ifnot1"),
                                    )
                            case "__COMPILER__IF_MESSAGE_FIELD_GROUP_IS_ONEOF__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                group = message.field_groups[env["__COMPILER__MESSAGE_FIELD_GROUP_NAME__"]]
                                assert len(group) > 0
                                if len(group) > 1:
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        env,
                                        (*annotations, "if1"),
                                    )
                            case "__COMPILER__IF_MESSAGE_FIELD_TYPE_NOT_REPEATED__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                field = message.field_for_number[int(env["__COMPILER__MESSAGE_FIELD_NUMBER__"])]
                                if not field.is_repeated:
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        env,
                                        (*annotations, "ifnotr"),
                                    )
                            case "__COMPILER__IF_MESSAGE_FIELD_TYPE_REPEATED__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                field = message.field_for_number[int(env["__COMPILER__MESSAGE_FIELD_NUMBER__"])]
                                if field.is_repeated and (
                                    field.submessage is None or field.submessage.map_types is None
                                ):
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        env,
                                        (*annotations, "ifr"),
                                    )
                            case "__COMPILER__IF_MESSAGE_FIELD_TYPE_MAP__":
                                mod = self.modules[env["__COMPILER__MODULE_NAME__"]]
                                message = mod.messages[env["__COMPILER__MESSAGE_PYTHON_NAME__"]]
                                field = message.field_for_number[int(env["__COMPILER__MESSAGE_FIELD_NUMBER__"])]
                                if field.submessage is not None and field.submessage.map_types is not None:
                                    replace_template_scope(
                                        line_num + 1,
                                        block_end_line - 1,
                                        env,
                                        (*annotations, "ifmap"),
                                    )

                        line_num = block_end_line
                        add_line_directive(line_num, annotations)
                        continue

                    # Replace inline tags with their values from the env
                    result_line = template_line
                    inline_tag_m = re_inline_tag.search(result_line)
                    while inline_tag_m is not None:
                        tag_start, tag_end = inline_tag_m.span()
                        try:
                            result_line = result_line[:tag_start] + env[inline_tag_m.group()] + result_line[tag_end:]
                        except KeyError:
                            logging.error(
                                "Missing key %r at line %d in template %s",
                                inline_tag_m.group(),
                                line_num + 1,
                                template_path,
                            )
                            raise
                        inline_tag_m = re_inline_tag.search(result_line)

                    result_lines.append(result_line)
                    line_num += 1

            except Exception:
                logging.error(
                    "Exception trace: %d in replace_template_scope(%d, %d, %r)",
                    line_num + 1,
                    start_line_num + 1,
                    end_line_num + 1,
                    env,
                )
                raise

        module_env = {
            "__COMPILER__BASE_MODULE_NAME__": so_module_name.split(".")[-1],
            "__COMPILER__QUALIFIED_MODULE_NAME__": so_module_name,
        }
        replace_template_scope(0, len(template_lines), module_env, [])

        result = "\n".join(result_lines)
        assert "__COMPILER__" not in result, "Some __COMPILER__ tags were not replaced"
        return result

    def pyi_source(self) -> str:
        lines = [
            "from __future__ import annotations",
            "from enum import IntEnum",
            "from typing import Any, TypeAlias",
            "",
        ]

        # The "classes" in the pyi file are actually modules in the C
        # extension, but they get the job done for typechecking
        for mod_name, mod_info in sorted(self.modules.items()):
            lines.append(f"class {mod_name}:")
            lines += mod_info.pyi_source_lines(indent_level=1)

        # Create global aliases as defined in the dicts
        lines.append("")
        lines.append("# Global aliases")
        for name, ent_info in self.global_aliases.items():
            if ent_info is None:
                lines.append(f"# Warning: {name} exists in multiple modules; cannot create global alias")
            else:
                lines.append(
                    f"{cc_name_for_python_name(name)}: TypeAlias = {ent_info.module_name}.{cc_name_for_python_name(ent_info.name)}"
                )

        lines.append("")
        return "\n".join(lines)


async def get_compiler_args() -> list[str]:
    (cflags, _), (ldflags, _) = await asyncio.gather(
        check_output_async("python3-config", "--cflags"),
        check_output_async("python3-config", "--ldflags"),
    )
    ret = [flag.decode("utf-8") for flag in cflags.split() + ldflags.split()]
    ret.append("-std=c++20")
    ret.append("-Wall")
    ret.append("-Wextra")
    ret.append("-Werror")
    ret.append("-Wno-error=missing-field-initializers")
    ret.append("-fPIC")
    return ret


async def compile_modules(
    output_basename: str,
    module_names: Iterable[str],
    add_line_directives: bool = True,
    compile_cc: bool = True,
) -> None:
    mod_coll = ModuleCollection(modules={})
    for module_name in module_names:
        mod_coll.add_file(importlib.import_module(module_name).DESCRIPTOR)
    mod_coll.compute_global_aliases()

    async def write_coll(output_basename: str, mod_coll: ModuleCollection) -> None:
        cc_filename = output_basename + ".cc"
        pyi_filename = output_basename + ".pyi"
        so_filename = output_basename + ".so"
        so_module_name = output_basename.replace("/", ".")

        print(f"Generating {pyi_filename}")
        with open(pyi_filename, "wt") as f:
            f.write(mod_coll.pyi_source())
        print(f"Wrote {pyi_filename}")

        print(f"Generating {cc_filename}")
        with open(cc_filename, "wt") as f:
            f.write(mod_coll.cc_source(so_module_name, add_line_directives=add_line_directives))
        print(f"Wrote {cc_filename}")

        if compile_cc:
            print(f"Compiling {cc_filename} to {so_filename}")
            py_compiler_args = await get_compiler_args()
            cmd = ["g++", *py_compiler_args, cc_filename, "-shared", "-o", so_filename]
            print("... " + " ".join(cmd))
            await check_call_async(*cmd)
            print(f"Compiled {so_filename}")

    await write_coll(output_basename, mod_coll)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("module_names", type=str, nargs="+")
    parser.add_argument("--no-line-directives", action="store_true", default=False)
    parser.add_argument("--source-only", action="store_true", default=False)
    parser.add_argument("--output-basename", type=str, required=True)
    parser.add_argument("--proto-files", action="store_true", default=False)
    args = parser.parse_args()

    if args.proto_files:
        with tempfile.TemporaryDirectory(dir=".") as temp_dir:
            tasks: list[Awaitable[Any]] = []
            temp_module_names: list[str] = []
            for proto_filename in args.module_names:
                tasks.append(
                    check_call_async(
                        sys.executable,
                        "-m",
                        "grpc_tools.protoc",
                        "-I.",
                        proto_filename,
                        f"--python_out={temp_dir}",
                        f"--pyi_out={temp_dir}",
                    )
                )
                temp_module_names.append(f"{os.path.basename(temp_dir)}.{proto_filename.removesuffix('.proto')}_pb2")
            await asyncio.gather(*tasks)
            await compile_modules(
                args.output_basename,
                temp_module_names,
                add_line_directives=not args.no_line_directives,
                compile_cc=not args.source_only,
            )
    else:
        await compile_modules(
            args.output_basename,
            args.module_names,
            add_line_directives=not args.no_line_directives,
            compile_cc=not args.source_only,
        )


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
