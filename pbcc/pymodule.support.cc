#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <Python.h>

#include "pymodule.support.hh"

void add_object_to_module(PyObject* base_module, const std::string& path, PyObject* obj) {
  Py_INCREF(base_module);
  PyObjectRef<> parent = base_module;

  std::string attr_name = path;
  size_t dot_pos = attr_name.find('.');
  while (dot_pos != std::string::npos) {
    std::string parent_name = attr_name.substr(0, dot_pos);
    attr_name = attr_name.substr(dot_pos + 1);

    PyObjectRef<> next_parent = PyObject_GetAttrString(parent.borrow(), parent_name.c_str());
    if (!next_parent) {
      throw python_error("");
    }
    parent = std::move(next_parent);
    dot_pos = attr_name.find('.');
  }

  if (PyModule_Check(parent.borrow())) {
    if (PyModule_AddObjectRef(parent.borrow(), attr_name.c_str(), obj)) {
      throw python_error("");
    }
  } else {
    if (PyObject_SetAttrString(parent.borrow(), attr_name.c_str(), obj)) {
      throw python_error("");
    }
  }
}

__attribute__((format(printf, 1, 2))) std::string string_printf(const char* fmt, ...) {
  va_list va;
  va_start(va, fmt);
  char* result = nullptr;
  int length = vasprintf(&result, fmt, va);
  if (result == nullptr) {
    throw std::bad_alloc();
  }
  // NOTE: It's not great that we copy the string again here, but this is only
  // used in error cases so it's probably not a big deal
  std::string ret(result, length);
  free(result);
  va_end(va);
  return ret;
}

StringReader::StringReader() : data(nullptr), length(0), offset(0) {}
StringReader::StringReader(const void* data, size_t size, size_t offset)
    : data(reinterpret_cast<const uint8_t*>(data)), length(size), offset(offset) {}

StringReader StringReader::subx(size_t offset) const {
  if (offset > this->length) {
    throw std::out_of_range("sub-reader begins beyond end of data");
  }
  return StringReader(reinterpret_cast<const char*>(this->data) + offset, this->length - offset);
}

StringReader StringReader::subx(size_t offset, size_t size) const {
  if (offset + size > this->length) {
    throw std::out_of_range("sub-reader begins or extends beyond end of data");
  }
  return StringReader(reinterpret_cast<const char*>(this->data) + offset, size);
}

std::string StringReader::preadx(size_t offset, size_t size) const {
  if (offset + size > this->length) {
    throw std::out_of_range("not enough data to read");
  }
  return std::string(reinterpret_cast<const char*>(this->data + offset), size);
}

std::string repr(PyObject* obj) {
  PyObjectRef<> repr = raise_python_errors(PyObject_Repr, obj);
  if (!PyUnicode_Check(repr.borrow())) {
    throw std::runtime_error("repr() returned something other than a unicode object");
  }
  return std::string(PyUnicode_AsUTF8(repr.borrow()));
}

void PyEnumRef::create_py_enum(const char* qualified_module_name) {
  PyObjectRef<> enum_module = raise_python_errors(PyImport_ImportModule, "enum");
  // NOTE: We intentionally don't use IntEnum here, to prevent users from accidentally assigning an enum value to an
  // int-valued field.
  PyObjectRef<> enum_class = raise_python_errors(PyObject_GetAttrString, enum_module.borrow(), "IntEnum");
  PyObjectRef<> enum_name = raise_python_errors(PyUnicode_FromString, this->get_python_name());
  PyObjectRef<> enum_members = raise_python_errors(PyDict_New);

  this->populate_values(enum_members.borrow());

  PyObjectRef<> args = raise_python_errors(PyTuple_Pack, 2, enum_name.borrow(), enum_members.borrow());
  PyObjectRef<> local_py_enum = raise_python_errors(PyObject_CallObject, enum_class.borrow(), args.borrow());

  // Populate the values map by iterating the constructed enum class
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, local_py_enum.borrow());
  while (PyObjectRef<> entry = PyIter_Next(it.borrow())) {
    PyObjectRef<> enum_value = raise_python_errors(PyObject_GetAttrString, entry.borrow(), "value");

    // Enum values can be negative in protobuf, and they just get encoded as unsigned 32-bit integers anyway (which is
    // inefficient). To implement this behavior, we treat the Python integer as signed, then immediately discard its
    // sign information.
    int64_t value = PyLong_AsLongLong(enum_value.borrow());
    if (!is_in_s32_range(value)) {
      throw std::runtime_error("Enum value outside of signed 32-bit range");
    } else if ((value != -1) || !PyErr_Occurred()) {
      auto& ref = this->py_enum_value_for_int_value[value];
      ref.assign_ref(entry.release());
      this->int_value_for_py_enum_value.emplace(ref.borrow(), value);
    } else {
      throw python_error("");
    }
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  }

  // It seems the enum members can't be pickled because the pickler can't look up which module they're in (it appears
  // as importlib._bootstrap) unless we do this.
  PyObjectRef<> module_name_str = raise_python_errors(PyUnicode_FromString, qualified_module_name);
  if (PyObject_SetAttrString(local_py_enum.borrow(), "__module__", module_name_str.borrow()) == -1) {
    throw python_error("");
  }

  this->py_enum.assign_ref(local_py_enum.release());
}

PyObject* py_dict_value_for_primitive_value(PyObject* obj) {
  // Hack: If the object has a .as_dict() method, call it and use the result. Otherwise, just use the object itself.
  int has_as_dict = PyObject_HasAttrString(obj, "as_dict");
  if (has_as_dict == -1) {
    throw python_error("");
  }
  if (has_as_dict) {
    PyObjectRef<> method = raise_python_errors(PyObject_GetAttrString, obj, "as_dict");
    // args must not be null (so we use an empty tuple) but kwargs can be null, according to the docs
    PyObjectRef<> args = raise_python_errors(PyTuple_New, 0);
    return raise_python_errors(PyObject_Call, method.borrow(), args.borrow(), nullptr);
  } else {
    Py_INCREF(obj);
    return obj;
  }
}

PyObject* py_dict_value_for_value(PyObject* obj) {
  if (PyList_Check(obj)) {
    ssize_t num_items = PyList_Size(obj);
    if (num_items < 0) {
      throw python_error("");
    }
    PyObjectRef<> ret = raise_python_errors(PyList_New, num_items);
    for (ssize_t z = 0; z < num_items; z++) {
      PyList_SET_ITEM(ret.borrow(), z, py_dict_value_for_value(PyList_GET_ITEM(obj, z)));
    }
    return ret.release();

  } else if (PyDict_Check(obj)) {
    PyObjectRef<> ret = raise_python_errors(PyDict_New);
    // key and value will be borrowed references, so we don't have to DECREF them
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    // TODO: In the free-threaded build, we'll need PY_BEGIN_CRITICAL_SECTION here, but that macro isn't (yet?)
    // compatible with C++. See https://docs.python.org/3/c-api/dict.html#c.PyDict_Next
    while (PyDict_Next(obj, &pos, &key, &value)) {
      PyObjectRef<> new_value = py_dict_value_for_value(value);
      if (PyDict_SetItem(ret.borrow(), key, new_value.borrow())) {
        throw python_error("");
      }
    }
    return ret.release();

  } else {
    return py_dict_value_for_primitive_value(obj);
  }
}

uint64_t decode_varint(StringReader& r) {
  uint8_t shift = 0;
  uint64_t ret = 0;
  for (;;) {
    if (shift >= 64) {
      throw std::runtime_error("varint has more than 10 7-bit digits");
    }
    uint8_t v = r.get_u8();
    ret |= (static_cast<uint64_t>(v & 0x7F) << shift);
    if (!(v & 0x80)) {
      return ret;
    }
    shift += 7;
  }
}

void encode_varint(StringWriter& w, uint64_t v) {
  while (v > 0x7F) {
    w.put_u8((v & 0x7F) | 0x80);
    v >>= 7;
  }
  // v cannot be zero here unless it was already zero before the loop
  w.put_u8(v);
}

[[noreturn]] void throw_incorrect_type(WireType expected_type, WireType received_type) {
  throw std::runtime_error(string_printf(
      "Incorrect type: expected %s, received %s",
      name_for_wire_type(expected_type), name_for_wire_type(received_type)));
}

PyObject* create_py_none() {
  Py_RETURN_NONE;
}
PyObject* create_py_false() {
  Py_RETURN_FALSE;
}
PyObject* create_py_int_zero() {
  return raise_python_errors(PyLong_FromLong, 0);
}
PyObject* create_py_float_zero() {
  return raise_python_errors(PyFloat_FromDouble, 0.0);
}
PyObject* create_py_empty_str() {
  return raise_python_errors(PyUnicode_FromStringAndSize, nullptr, 0);
}
PyObject* create_py_empty_bytes() {
  return raise_python_errors(PyBytes_FromStringAndSize, nullptr, 0);
}
PyObject* create_py_empty_list() {
  return raise_python_errors(PyList_New, 0);
}
PyObject* create_py_empty_dict() {
  return raise_python_errors(PyDict_New);
}

template <>
void serialize_with_tag<DataType::MESSAGE>(StringWriter& w, uint64_t field_num, DefaultBehavior default_behavior, PyObject* obj, PyEnumRef*, SerializeMessageFn serialize_message) {
  if ((default_behavior == DefaultBehavior::OPTIONAL) && (obj == Py_None)) {
    return;
  }
  if (!serialize_message) {
    throw std::logic_error("Serializer not available for submessage");
  }
  StringWriter sub_w;
  serialize_message(obj, sub_w);
  if ((sub_w.size() == 0) && (default_behavior == DefaultBehavior::REQUIRED)) {
    // The submessage had no non-default values and is not optional; no need to serialize anything
    return;
  }
  encode_varint(w, encode_tag(field_num, wire_type_for_data_type(DataType::MESSAGE)));
  encode_varint(w, sub_w.size());
  w.write(sub_w.str());
}

template <>
void serialize_oneof_with_tag<DataType::UNKNOWN>(StringWriter&, PyObject* obj, const SerializeOneofParams*) {
  if (obj != Py_None) {
    throw std::runtime_error("Value for oneof field was not any of the expected types");
  }
}

void skip_field(StringReader& r, WireType type) {
  switch (type) {
    case WireType::VARINT:
      decode_varint(r);
      break;
    case WireType::INT64:
      r.skip(8);
      break;
    case WireType::LENGTH:
      r.skip(decode_varint(r));
      break;
    case WireType::INT32:
      r.skip(4);
      break;
    default:
      throw std::runtime_error(string_printf("Unknown field type %02hhX", static_cast<uint8_t>(type)));
  }
}
