#pragma once

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

constexpr ssize_t REPR_STRING_MAX_CHARACTERS = 10000;
constexpr ssize_t REPR_STRING_MAX_BYTES = 100;

class python_error : public std::runtime_error {
public:
  using runtime_error::runtime_error;
};

template <typename Func, typename... ArgTs>
PyObject* raise_python_errors(Func&& func, ArgTs&&... args) {
  PyObject* ret = std::forward<Func>(func)(std::forward<ArgTs>(args)...);
  if (ret == nullptr) {
    throw python_error("");
  }
  return ret;
}

template <typename Func, typename... ArgTs>
PyObject* handle_python_errors(Func&& func, ArgTs&&... args) {
  try {
    return reinterpret_cast<PyObject*>(std::forward<Func>(func)(std::forward<ArgTs>(args)...));
  } catch (const python_error& e) {
    if (!PyErr_Occurred()) {
      throw std::logic_error("python_error exception caught without Python error state set");
    }

    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);

    if (value) {
      PyObject* prefixed_message = PyUnicode_FromFormat("%s%S", e.what(), value);
      PyErr_Restore(type, prefixed_message, traceback);
      Py_DECREF(value);
    } else {
      PyErr_Restore(type, value, traceback);
    }
    return nullptr;

  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
}

void add_object_to_module(PyObject* base_module, const std::string& path, PyObject* obj);

constexpr bool is_in_u32_range(uint64_t v) {
  return (v & 0xFFFFFFFF00000000LL) == 0;
}

constexpr bool is_in_s32_range(int64_t v) {
  return ((v >= -0x80000000LL) && (v <= 0x7FFFFFFFLL));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// String reader/writer (from phosg)

__attribute__((format(printf, 1, 2))) std::string string_printf(const char* fmt, ...);

// Try to determine endianess from GCC defines first. If they aren't available, use some constants to figure it out
// clang-format off
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  // OK; system is little-endian
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  #error pbcc cannot be compiled on big-endian systems (for now)  
#else
  #define LITTLE_ENDIAN_VALUE 0x31323334UL
  #define BIG_ENDIAN_VALUE    0x34333231UL
  #define ENDIAN_ORDER_VALUE  ('1234')
  #if ENDIAN_ORDER_VALUE == LITTLE_ENDIAN_VALUE
  // OK; system is little-endian
  #elif ENDIAN_ORDER_VALUE == BIG_ENDIAN_VALUE
    #error pbcc cannot be compiled on big-endian systems (for now)
  #else
    #error "Unrecognized host system endianness"
  #endif
  #undef LITTLE_ENDIAN_VALUE
  #undef BIG_ENDIAN_VALUE
  #undef ENDIAN_ORDER_VALUE
#endif
// clang-format on

class StringReader {
public:
  StringReader();
  StringReader(const void* data, size_t size, size_t offset = 0);
  virtual ~StringReader() = default;

  inline size_t where() const {
    return this->offset;
  }
  inline size_t size() const {
    return this->length;
  }
  inline size_t remaining() const {
    return this->length - this->offset;
  }
  inline void go(size_t offset) {
    this->offset = offset;
  }
  inline void skip(size_t bytes) {
    this->offset += bytes;
    if (this->offset > this->length) {
      this->offset = this->length;
      throw std::out_of_range("skip beyond end of string");
    }
  }
  inline bool eof() const {
    return (this->offset >= this->length);
  }

  StringReader subx(size_t offset) const;
  StringReader subx(size_t offset, size_t size) const;
  std::string preadx(size_t offset, size_t size) const;

  inline const void* pgetv(size_t offset, size_t size) const {
    if (offset + size > this->length) {
      throw std::out_of_range("end of string");
    }
    return this->data + offset;
  }
#if defined(__x86_64__) || defined(_M_X64)
  template <typename T>
  const T& pget(size_t offset, size_t size = sizeof(T)) const {
    return *reinterpret_cast<const T*>(this->pgetv(offset, size));
  }
#else
  template <typename T>
  T pget(size_t offset, size_t size = sizeof(T)) const {
    T ret;
    memcpy(&ret, this->pgetv(offset, size), size);
    return ret;
  }
#endif

  inline const void* getv(size_t size, bool advance = true) {
    const void* ret = this->pgetv(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }

#if defined(__x86_64__) || defined(_M_X64)
  template <typename T>
  const T& get(bool advance = true, size_t size = sizeof(T)) {
    const T& ret = this->pget<T>(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }
#else
  template <typename T>
  T get(bool advance = true, size_t size = sizeof(T)) {
    T ret = this->pget<T>(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }
#endif

  // TODO: These should use the le_ types if we ever build this on big-endian systems
  inline uint8_t get_u8(bool advance = true) { return this->get<uint8_t>(advance); }
  inline int8_t get_s8(bool advance = true) { return this->get<int8_t>(advance); }
  inline uint16_t get_u16l(bool advance = true) { return this->get<uint16_t>(advance); }
  inline int16_t get_s16l(bool advance = true) { return this->get<int16_t>(advance); }
  inline uint32_t get_u32l(bool advance = true) { return this->get<uint32_t>(advance); }
  inline int32_t get_s32l(bool advance = true) { return this->get<int32_t>(advance); }
  inline uint64_t get_u64l(bool advance = true) { return this->get<uint64_t>(advance); }
  inline int64_t get_s64l(bool advance = true) { return this->get<int64_t>(advance); }
  inline float get_f32l(bool advance = true) { return this->get<float>(advance); }
  inline double get_f64l(bool advance = true) { return this->get<double>(advance); }

private:
  const uint8_t* data;
  size_t length;
  size_t offset;
};

class StringWriter {
public:
  StringWriter() = default;
  ~StringWriter() = default;

  inline size_t size() const {
    return this->data.size();
  }

  inline void write(const void* data, size_t size) {
    this->data.append(reinterpret_cast<const char*>(data), size);
  }
  inline void write(const std::string& data) {
    this->data.append(data);
  }

  template <typename T>
  void put(const T& v) {
    this->write(reinterpret_cast<const char*>(&v), sizeof(v));
  }

  // TODO: These should use the le_ types if we ever build this on big-endian systems
  inline void put_u8(uint8_t v) { this->data.push_back(static_cast<char>(v)); }
  inline void put_s8(int8_t v) { this->data.push_back(v); }
  inline void put_u16l(uint16_t v) { this->put<uint16_t>(v); }
  inline void put_s16l(int16_t v) { this->put<int16_t>(v); }
  inline void put_u32l(uint32_t v) { this->put<uint32_t>(v); }
  inline void put_s32l(int32_t v) { this->put<int32_t>(v); }
  inline void put_u64l(uint64_t v) { this->put<uint64_t>(v); }
  inline void put_s64l(int64_t v) { this->put<int64_t>(v); }
  inline void put_f32l(float v) { this->put<float>(v); }
  inline void put_f64l(double v) { this->put<double>(v); }

  inline std::string& str() {
    return this->data;
  }
  inline const std::string& str() const {
    return this->data;
  }

private:
  std::string data;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Object references

// This class holds a reference to a PyObject. When constructed, this class takes ownership of the passed-in reference
// (the caller should NOT call Py_DECREF on it). This can be thought of as an analogue to std::shared_ptr, but for
// Python objects.
// NOTE: There is no operator PyObject*, and this is intentional - we want the caller to have to think about whether
// they want to borrow the reference or make a new reference, so they must call .borrow() or .new_ref() to get it.
template <typename TargetT = PyObject>
struct PyObjectRef {
  TargetT* obj;

  PyObjectRef() : obj(nullptr) {}
  PyObjectRef(TargetT* obj) : obj(obj) {}
  ~PyObjectRef() {
    this->clear();
  }

  // Technically these could be made copyable, but we don't really need that functionality, and leaving the copy
  // constructors deleted allows us to detect extraneous increfs/decrefs.
  PyObjectRef(const PyObjectRef& other) = delete;
  PyObjectRef& operator=(const PyObjectRef& other) = delete;

  PyObjectRef(PyObjectRef&& other) : obj(other.obj) {
    other.obj = nullptr;
  }
  PyObjectRef& operator=(PyObjectRef&& other) {
    PyObject* prev_obj = this->obj;
    this->obj = other.obj;
    other.obj = nullptr;
    Py_XDECREF(prev_obj);
    return *this;
  }

  operator bool() const {
    return !!this->obj;
  }
  void assign_ref(TargetT* obj) {
    TargetT* prev_obj = this->obj;
    this->obj = obj;
    Py_XDECREF(prev_obj);
  }
  TargetT* borrow() const {
    return this->obj;
  }
  TargetT* new_ref() const {
    Py_INCREF(this->obj);
    return this->obj;
  }
  TargetT* release() {
    TargetT* ret = this->obj;
    this->obj = nullptr;
    return ret;
  }
  TargetT* operator->() const {
    return this->obj;
  }
  inline void clear() {
    this->assign_ref(nullptr);
  }
};
static_assert(sizeof(PyObjectRef<>) == sizeof(PyObject*), "PyObjectRef contains more than just a single pointer");

std::string repr(PyObject* obj);

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Enums

// This class holds a reference to a constructed Python Enum class and allows fast native lookups of the values and
// Python value objects. This should be subclassed and instantiated once for each enum the caller intends to use.
class PyEnumRef {
public:
  void create_py_enum(const char* qualified_module_name);

  inline const PyObjectRef<>& py_enum_class() {
    return this->py_enum;
  }
  inline bool has_py_member(const PyObject* obj) const {
    return this->int_value_for_py_enum_value.count(obj);
  }
  inline const PyObjectRef<>& py_member_for_value(int64_t value) const {
    try {
      return this->py_enum_value_for_int_value.at(value);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(string_printf("Enum member %" PRIu64 " does not exist", value));
    }
  }
  inline int64_t value_for_py_member(const PyObject* obj) const {
    try {
      return this->int_value_for_py_enum_value.at(obj);
    } catch (const std::out_of_range&) {
      throw std::runtime_error("Value is not an enum member");
    }
  }

protected:
  virtual const char* get_python_name() const = 0;
  virtual void populate_values(PyObject* dict) = 0;

private:
  std::unordered_map<const PyObject*, int64_t> int_value_for_py_enum_value;
  std::unordered_map<int64_t, PyObjectRef<>> py_enum_value_for_int_value;
  PyObjectRef<> py_enum;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Python C API utility functions

PyObject* py_dict_value_for_primitive_value(PyObject* obj);
PyObject* py_dict_value_for_value(PyObject* obj);

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Protobuf definitions

enum class WireType {
  UNKNOWN = -1,

  // Field contents are another varint-encoded integer, zigzag-encoded if the type is signed (sint32 or sint64).
  // Used for int32, int64, uint32, uint64, sint32, sint64, bool, enum
  VARINT = 0,
  // Field contents are 8 bytes, little-endian.
  // Used for fixed64, sfixed64, double
  INT64 = 1,
  // Field contents are a varint specifying how many data bytes follow, followed immediately by the data bytes. The
  // number of items in a packed repeated field is not specified; the parser should continue parsing items until it
  // reads the entire data string.
  // Used for string, bytes, embedded messages, packed repeated fields
  LENGTH = 2,
  // We don't support groups since they're deprecated.
  GROUP_START = 3,
  GROUP_END = 4,
  // Field contents are 4 bytes, little-endian.
  // Used for fixed32, sfixed32, float
  INT32 = 5,
};

constexpr const char* name_for_wire_type(WireType t) {
  switch (t) {
    case WireType::VARINT:
      return "VARINT";
    case WireType::INT64:
      return "INT64";
    case WireType::LENGTH:
      return "LENGTH";
    case WireType::GROUP_START:
      return "GROUP_START";
    case WireType::GROUP_END:
      return "GROUP_END";
    case WireType::INT32:
      return "INT32";
    default:
      return "__UNKNOWN__";
  }
}

enum class DataType {
  UNKNOWN = -1,
  FLOAT,
  DOUBLE,
  INT32,
  UINT32,
  SINT32,
  INT64,
  UINT64,
  SINT64,
  FIXED32,
  SFIXED32,
  FIXED64,
  SFIXED64,
  BOOL,
  ENUM,
  STRING,
  BYTES,
  MAP, // message_constructor required in parse()
  MESSAGE, // message_constructor required in parse()
};

constexpr bool is_uint_data_type(DataType t) {
  return ((t == DataType::UINT32) ||
      (t == DataType::UINT64) ||
      (t == DataType::BOOL) ||
      (t == DataType::ENUM) ||
      (t == DataType::FIXED32) ||
      (t == DataType::FIXED64));
}
constexpr bool is_sint_data_type(DataType t) {
  return ((t == DataType::INT32) ||
      (t == DataType::SINT32) ||
      (t == DataType::INT64) ||
      (t == DataType::SINT64) ||
      (t == DataType::SFIXED32) ||
      (t == DataType::SFIXED64));
}
constexpr bool is_float_data_type(DataType t) {
  return ((t == DataType::FLOAT) || (t == DataType::DOUBLE));
}

constexpr bool is_varint_data_type(DataType t) {
  return ((t == DataType::INT32) ||
      (t == DataType::UINT32) ||
      (t == DataType::SINT32) ||
      (t == DataType::INT64) ||
      (t == DataType::UINT64) ||
      (t == DataType::SINT64) ||
      (t == DataType::BOOL) ||
      (t == DataType::ENUM));
}
constexpr bool is_int32_data_type(DataType t) {
  return ((t == DataType::FLOAT) || (t == DataType::FIXED32) || (t == DataType::SFIXED32));
}
constexpr bool is_int64_data_type(DataType t) {
  return ((t == DataType::DOUBLE) || (t == DataType::FIXED64) || (t == DataType::SFIXED64));
}
constexpr bool is_string_data_type(DataType t) {
  return ((t == DataType::STRING) || (t == DataType::BYTES));
}
constexpr bool is_message_data_type(DataType t) {
  return ((t == DataType::MAP) || (t == DataType::MESSAGE));
}
constexpr bool can_use_packed_repeated_format(DataType t) {
  // String data types can't be serialized in the packed format, since they couldn't be distinguished on the wire from
  // non-packed string data types if that were allowed
  return (!is_string_data_type(t) && (t != DataType::MAP) && (t != DataType::MESSAGE));
}

template <DataType data_type>
  requires(is_uint_data_type(data_type) && data_type != DataType::BOOL && data_type != DataType::ENUM)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyLong_Check(obj)) {
    return false;
  }
  uint64_t ret = PyLong_AsUnsignedLongLong(obj);
  if (ret == 0) {
    return true;
  } else if ((ret != static_cast<uint64_t>(-1)) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_sint_data_type(data_type))
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyLong_Check(obj)) {
    return false;
  }
  int64_t ret = PyLong_AsLongLong(obj);
  if (ret == 0) {
    return true;
  } else if ((ret != -1) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_float_data_type(data_type))
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyFloat_Check(obj) && !PyLong_Check(obj)) {
    return false;
  }
  double ret = PyFloat_AsDouble(obj);
  if (ret == 0.0) {
    return true;
  } else if ((ret != -1.0) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(data_type == DataType::BOOL)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  return PyBool_Check(obj) && Py_IsFalse(obj);
}
template <DataType data_type>
  requires(data_type == DataType::ENUM)
bool obj_has_default_value(PyObject* obj, const PyEnumRef* enum_ref) {
  try {
    return (enum_ref->value_for_py_member(obj) == 0);
  } catch (const std::runtime_error&) {
    return false;
  }
}
template <DataType data_type>
  requires(data_type == DataType::STRING)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyUnicode_Check(obj)) {
    return false;
  }
  ssize_t length = PyUnicode_GetLength(obj);
  if (length == 0) {
    return true;
  } else if (length != -1.0) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(data_type == DataType::BYTES)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyBytes_Check(obj)) {
    return false;
  }
  ssize_t length = PyBytes_Size(obj);
  if (length == 0) {
    return true;
  } else if (length != -1.0) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_message_data_type(data_type))
bool obj_has_default_value(PyObject*, const PyEnumRef*) {
  // TODO: This shouldn't always return false. This shouldn't cause any correctness issues but probably causes us to
  // waste some space.
  return false;
}

constexpr WireType wire_type_for_data_type(DataType t) {
  switch (t) {
    case DataType::FIXED32:
    case DataType::SFIXED32:
    case DataType::FLOAT:
      return WireType::INT32;
    case DataType::FIXED64:
    case DataType::SFIXED64:
    case DataType::DOUBLE:
      return WireType::INT64;
    case DataType::INT32:
    case DataType::UINT32:
    case DataType::SINT32:
    case DataType::INT64:
    case DataType::UINT64:
    case DataType::SINT64:
    case DataType::BOOL:
    case DataType::ENUM:
      return WireType::VARINT;
    case DataType::STRING:
    case DataType::BYTES:
    case DataType::MAP:
    case DataType::MESSAGE:
      return WireType::LENGTH;
    default:
      return WireType::UNKNOWN;
  }
}

constexpr WireType wire_type_for_tag(uint64_t tag) {
  return static_cast<WireType>(tag & 7);
}
constexpr uint64_t field_num_for_tag(uint64_t tag) {
  return tag >> 3;
}
constexpr uint64_t encode_tag(uint64_t field_num, WireType type) {
  return (field_num << 3) | static_cast<uint64_t>(type);
}

uint64_t decode_varint(StringReader& r);
void encode_varint(StringWriter& w, uint64_t v);

inline int64_t decode_varint_signed(StringReader& r) {
  uint64_t v = decode_varint(r);
  return (v >> 1) ^ ((v & 1) ? -1 : 0);
}

inline void encode_varint_signed32(StringWriter& w, int32_t n) {
  encode_varint(w, static_cast<uint32_t>((n << 1) ^ (n >> 31)));
}

inline void encode_varint_signed64(StringWriter& w, int64_t n) {
  encode_varint(w, (n << 1) ^ (n >> 63));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Field codecs

enum ParseFlag {
  RETAIN_UNKNOWN_FIELDS = 0x01,
  IGNORE_INCORRECT_TYPES = 0x02,
};

using ParseMessageFn = PyObject* (*)(const void* data, size_t size, uint8_t flags);
using SerializeMessageFn = void (*)(PyObject* obj, StringWriter&);

[[noreturn]] void throw_incorrect_type(WireType expected_type, WireType received_type);

PyObject* create_py_none();
PyObject* create_py_false();
PyObject* create_py_int_zero();
PyObject* create_py_float_zero();
PyObject* create_py_empty_str();
PyObject* create_py_empty_bytes();
PyObject* create_py_empty_list();
PyObject* create_py_empty_dict();

template <DataType...>
struct AlwaysFalse {
  static constexpr bool v = false;
};

template <DataType data_type>
struct TypeCodec {
  static bool value_matches_type(PyObject*, PyEnumRef*, PyTypeObject*, bool) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::value_matches_type should never be called");
    return false;
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::construct_default should never be called");
    return nullptr;
  }
  static PyObject* parse(StringReader&, PyEnumRef*, ParseMessageFn, uint8_t) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::parse should never be called");
    return nullptr;
  }
  static void serialize_without_tag(StringWriter&, PyObject*, PyEnumRef*, SerializeMessageFn) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::serialize_without_tag should never be called");
  }
};

template <>
struct TypeCodec<DataType::INT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, static_cast<int32_t>(decode_varint(r)));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of signed 32-bit range");
    }
    // Note: It appears Google's protobuf library encodes this as if it were a 64-bit integer, so -1 is encoded as 10
    // bytes instead of 5 bytes. We do the same here, even though it's probably wrong.
    encode_varint(w, static_cast<uint64_t>(v));
  }
};

template <>
struct TypeCodec<DataType::UINT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_u32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    encode_varint(w, v);
  }
};

template <>
struct TypeCodec<DataType::SINT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, decode_varint_signed(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of signed 32-bit range");
    }
    encode_varint_signed32(w, v);
  }
};

template <>
struct TypeCodec<DataType::INT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, static_cast<int64_t>(decode_varint(r)));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint(w, static_cast<uint64_t>(v));
  }
};

template <>
struct TypeCodec<DataType::UINT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLongLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint(w, v);
  }
};

template <>
struct TypeCodec<DataType::SINT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, decode_varint_signed(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint_signed64(w, v);
  }
};

template <>
struct TypeCodec<DataType::FIXED32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLong, r.get_u32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_u32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    w.put_u32l(v);
  }
};

template <>
struct TypeCodec<DataType::SFIXED32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, r.get_s32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    w.put_s32l(v);
  }
};

template <>
struct TypeCodec<DataType::FIXED64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLongLong, r.get_u64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_u64l(v);
  }
};

template <>
struct TypeCodec<DataType::SFIXED64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, r.get_s64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_s64l(v);
  }
};

template <>
struct TypeCodec<DataType::BOOL> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyBool_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_false();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyBool_FromLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    if (obj == Py_True) {
      w.put_u8(0x01);
    } else if (obj == Py_False) {
      w.put_u8(0x00);
    } else {
      throw std::invalid_argument("Boolean value was neither True nor False");
    }
  }
};

template <>
struct TypeCodec<DataType::FLOAT> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyFloat_Check(obj) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_float_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyFloat_FromDouble, r.get_f32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    float v = PyFloat_AsDouble(obj);
    if (v == -1.0 && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_f32l(v);
  }
};

template <>
struct TypeCodec<DataType::DOUBLE> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyFloat_Check(obj) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_float_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyFloat_FromDouble, r.get_f64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    double v = PyFloat_AsDouble(obj);
    if (v == -1.0 && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_f64l(v);
  }
};

template <>
struct TypeCodec<DataType::BYTES> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyBytes_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_empty_bytes();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    uint64_t size = decode_varint(r);
    return raise_python_errors(PyBytes_FromStringAndSize, reinterpret_cast<const char*>(r.getv(size)), size);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    char* data;
    ssize_t size;
    if (PyBytes_AsStringAndSize(obj, &data, &size)) {
      throw python_error("");
    }
    encode_varint(w, size);
    w.write(data, size);
  }
};

template <>
struct TypeCodec<DataType::STRING> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyUnicode_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_empty_str();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    uint64_t size = decode_varint(r);
    return raise_python_errors(PyUnicode_FromStringAndSize, reinterpret_cast<const char*>(r.getv(size)), size);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(obj, &size);
    if (!data) {
      throw python_error("");
    }
    encode_varint(w, size);
    w.write(data, size);
  }
};

template <>
struct TypeCodec<DataType::ENUM> {
  static bool value_matches_type(PyObject* obj, PyEnumRef* enum_ref, PyTypeObject*, bool is_optional) {
    if (!enum_ref) {
      throw std::logic_error("Enum definition is missing");
    }
    return (is_optional && (obj == Py_None)) || enum_ref->has_py_member(obj);
  }
  static PyObject* construct_default(PyEnumRef* enum_ref, ParseMessageFn) {
    return enum_ref->py_member_for_value(0).new_ref();
  }
  static PyObject* parse(StringReader& r, PyEnumRef* enum_ref, ParseMessageFn, uint8_t) {
    if (!enum_ref) {
      throw std::logic_error("Enum definition is missing");
    }
    int64_t v = static_cast<int64_t>(decode_varint(r));
    return enum_ref->py_member_for_value(v).new_ref();
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef* enum_ref, SerializeMessageFn) {
    encode_varint(w, enum_ref->value_for_py_member(obj));
  }
};

template <>
struct TypeCodec<DataType::MESSAGE> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject* type, bool is_optional) {
    if (is_optional && (obj == Py_None)) {
      return true;
    }
    int is_this_type = PyObject_IsInstance(obj, reinterpret_cast<PyObject*>(type));
    if (is_this_type == 1) {
      return true;
    } else if (is_this_type == 0) {
      return false;
    } else {
      throw python_error("");
    }
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn parse) {
    return parse(nullptr, 0, false);
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn parse_message, uint8_t flags) {
    uint64_t size = decode_varint(r);
    if (!parse_message) {
      throw std::logic_error("Parser not available for submessage");
    }
    return parse_message(r.getv(size), size, flags);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn serialize_message) {
    if (!serialize_message) {
      throw std::logic_error("Serializer not available for submessage");
    }
    StringWriter sub_w;
    serialize_message(obj, sub_w);
    encode_varint(w, sub_w.size());
    w.write(sub_w.str());
  }
};

enum class DefaultBehavior {
  // Don't write anything if the value is None
  OPTIONAL = 0,
  // Don't write anything if the value is the default
  REQUIRED,
  // Always write the value, even if it's the default (this is needed to make sure that default values in repeated
  // fields are serialized properly)
  ALWAYS_WRITE,
};

// Serializes a field AND its tag, unless its value is the default value
template <DataType data_type>
void serialize_with_tag(StringWriter& w, uint64_t field_num, DefaultBehavior default_behavior, PyObject* obj, PyEnumRef* enum_ref, SerializeMessageFn serialize_message) {
  // Optional fields are typed as `X | None`; if it's None, serialize nothing. Non-optional fields cannot be None, so
  // serialize nothing if the field has its default value.
  bool should_write;
  switch (default_behavior) {
    case DefaultBehavior::OPTIONAL:
      should_write = (obj != Py_None);
      break;
    case DefaultBehavior::REQUIRED:
      should_write = !obj_has_default_value<data_type>(obj, enum_ref);
      break;
    case DefaultBehavior::ALWAYS_WRITE:
      should_write = true;
      break;
    default:
      throw std::logic_error("invalid default behavior");
  }
  if (should_write) {
    encode_varint(w, encode_tag(field_num, wire_type_for_data_type(data_type)));
    TypeCodec<data_type>::serialize_without_tag(w, obj, enum_ref, serialize_message);
  }
}

template <>
void serialize_with_tag<DataType::MESSAGE>(StringWriter& w, uint64_t field_num, DefaultBehavior default_behavior, PyObject* obj, PyEnumRef*, SerializeMessageFn serialize_message);

// Repeated field parsing/serializing

template <DataType data_type>
void parse_packed_repeated(PyObject* list, StringReader& r, PyEnumRef* enum_ref, ParseMessageFn parse_message, uint8_t flags) {
  // Get the length, then parse as many items as possible from the following bytes and append them all to the list
  uint64_t size = decode_varint(r);
  StringReader sub_r = r.subx(r.where(), size);
  r.skip(size);
  while (!sub_r.eof()) {
    PyObjectRef<> v = TypeCodec<data_type>::parse(sub_r, enum_ref, parse_message, flags);
    if (PyList_Append(list, v.borrow())) {
      throw python_error("");
    }
  }
}

template <DataType data_type>
void parse_unpacked_repeated(PyObject* list, StringReader& r, PyEnumRef* enum_ref, ParseMessageFn parse_message, uint8_t flags) {
  // Parse a single item and append it to the list
  PyObjectRef<> v = TypeCodec<data_type>::parse(r, enum_ref, parse_message, flags);
  if (PyList_Append(list, v.borrow())) {
    throw python_error("");
  }
}

template <DataType data_type>
  requires(is_int32_data_type(data_type) || is_int64_data_type(data_type))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef*, SerializeMessageFn, PyTypeObject*) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  ssize_t num_items = PyList_Size(list);
  if (num_items == 0) {
    return;
  } else if (num_items < 0) {
    throw python_error("");
  }

  // Serialize in packed repeated format (LENGTH), with initially-known size
  encode_varint(w, encode_tag(field_num, WireType::LENGTH));
  size_t data_size = num_items * (is_int64_data_type(data_type) ? 8 : 4);
  encode_varint(w, data_size);

  size_t end_offset = w.size() + data_size;
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), nullptr, nullptr, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      TypeCodec<data_type>::serialize_without_tag(w, item.borrow(), nullptr, nullptr);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  } else if (end_offset != w.size()) {
    throw std::runtime_error("Serialized size does not match expected size");
  }
}
template <DataType data_type>
  requires(is_varint_data_type(data_type))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef* enum_ref, SerializeMessageFn, PyTypeObject*) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  ssize_t num_items = PyList_Size(list);
  if (num_items == 0) {
    return;
  } else if (num_items < 0) {
    throw python_error("");
  }

  // Serialize in packed repeated format (LENGTH), with initially-unknown size
  StringWriter items_w;
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), enum_ref, nullptr, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      TypeCodec<data_type>::serialize_without_tag(items_w, item.borrow(), enum_ref, nullptr);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  }

  encode_varint(w, encode_tag(field_num, WireType::LENGTH));
  encode_varint(w, items_w.size());
  w.write(items_w.str());
}

template <DataType data_type>
  requires(is_string_data_type(data_type) || (data_type == DataType::MESSAGE))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef*, SerializeMessageFn serialize_message, PyTypeObject* py_message_type) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  // Serialize in standard (non-packed) repeated format
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), nullptr, py_message_type, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      serialize_with_tag<data_type>(w, field_num, DefaultBehavior::ALWAYS_WRITE, item.borrow(), nullptr, serialize_message);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  }
}

// Map field parsing/serializing

template <DataType key_type, DataType value_type>
void parse_map(
    PyObject* dict,
    StringReader& r,
    PyEnumRef* value_enum_ref,
    ParseMessageFn value_parse_message,
    uint8_t flags) {
  // We don't bother with "proper" message decoding here, since the key and value types are known and there can only be
  // two fields in the submessage.
  uint64_t size = decode_varint(r);
  StringReader sub_r = r.subx(r.where(), size);
  r.skip(size);
  PyObjectRef<> key, value;
  while (!sub_r.eof()) {
    uint64_t tag = decode_varint(sub_r);
    WireType wire_type = wire_type_for_tag(tag);
    uint64_t field_num = field_num_for_tag(tag);
    // TODO: It'd be nice to store unknown fields due to incorrect types; currently we always raise in such situations
    if (field_num == 1) {
      if (wire_type != wire_type_for_data_type(key_type)) {
        throw_incorrect_type(wire_type_for_data_type(key_type), wire_type);
      }
      key.assign_ref(TypeCodec<key_type>::parse(sub_r, nullptr, nullptr, flags));
    } else if (field_num == 2) {
      if (wire_type != wire_type_for_data_type(value_type)) {
        throw_incorrect_type(wire_type_for_data_type(value_type), wire_type);
      }
      value.assign_ref(TypeCodec<value_type>::parse(sub_r, value_enum_ref, value_parse_message, flags));
    }
  }
  // If either the key or value is missing, parse an empty string to construct the default value
  if (!key) {
    key.assign_ref(TypeCodec<key_type>::construct_default(nullptr, nullptr));
  }
  if (!value) {
    value.assign_ref(TypeCodec<value_type>::construct_default(value_enum_ref, value_parse_message));
  }
  if (PyDict_SetItem(dict, key.borrow(), value.borrow())) {
    throw python_error("");
  }
}
template <DataType key_type, DataType value_type>
void serialize_map_with_tag(
    StringWriter& w,
    uint64_t field_num,
    PyObject* dict,
    PyEnumRef* value_enum_ref,
    SerializeMessageFn value_serialize_message,
    PyTypeObject* py_value_message_type) {
  if (!PyDict_Check(dict)) {
    throw std::runtime_error("Value is not a dictionary");
  }

  // key and value will be borrowed references, so we don't have to DECREF them
  PyObject* key;
  PyObject* value;
  Py_ssize_t pos = 0;
  // TODO: In the free-threaded build, we'll need PY_BEGIN_CRITICAL_SECTION here, but that macro isn't (yet?)
  // compatible with C++. See https://docs.python.org/3/c-api/dict.html#c.PyDict_Next
  while (PyDict_Next(dict, &pos, &key, &value)) {
    if (!TypeCodec<key_type>::value_matches_type(key, nullptr, nullptr, false)) {
      throw std::runtime_error("Incorrect data type for key field: " + repr(key));
    }
    if (!TypeCodec<value_type>::value_matches_type(value, value_enum_ref, py_value_message_type, false)) {
      throw std::runtime_error("Incorrect data type for value field: " + repr(value));
    }
    // Technically there should be a sub-message here, but we just cheese it since it would be annoying to implement
    // "properly". The message will always have fields 1 (key) and 2 (value), according to official protobuf docs.
    StringWriter item_w;
    // Apparently Google's protobuf library always writes these fields, even if they have the default values, so we do
    // so here too.
    serialize_with_tag<key_type>(item_w, 1, DefaultBehavior::ALWAYS_WRITE, key, nullptr, nullptr);
    serialize_with_tag<value_type>(item_w, 2, DefaultBehavior::ALWAYS_WRITE, value, value_enum_ref, value_serialize_message);
    encode_varint(w, encode_tag(field_num, WireType::LENGTH));
    encode_varint(w, item_w.size());
    w.write(item_w.str());
  }
}

// Oneof serializing (parsing doesn't require any special logic, but for serializing, we have to use isinstance() to
// figure out what to serialize)

struct SerializeOneofParams {
  uint64_t field_num = 0;
  bool is_optional = false;
  PyEnumRef* enum_ref = nullptr;
  SerializeMessageFn serialize_message = nullptr;
  PyTypeObject* message_type_obj = nullptr;
};

// Recursive case: serialize it if it's the first type; if it's not, try the remaining types recursively
template <DataType data_type, DataType... RemainingTs>
void serialize_oneof_with_tag(StringWriter& w, PyObject* obj, const SerializeOneofParams* params) {
  if (TypeCodec<data_type>::value_matches_type(obj, params->enum_ref, params->message_type_obj, false)) {
    auto default_behavior = params->is_optional ? DefaultBehavior::OPTIONAL : DefaultBehavior::REQUIRED;
    serialize_with_tag<data_type>(w, params->field_num, default_behavior, obj, params->enum_ref, params->serialize_message);
  } else {
    serialize_oneof_with_tag<RemainingTs...>(w, obj, params + 1);
  }
}

// Base case: no types matched (the caller always puts UNKNOWN at the end of the template args). This happens if the
// value is unset (None) or if the value's type doesn't match any of the oneof options. In the case where it's None, we
// should just serialize nothing.
template <>
void serialize_oneof_with_tag<DataType::UNKNOWN>(StringWriter&, PyObject*, const SerializeOneofParams*);

// Skip a field's data without parsing it
void skip_field(StringReader& r, WireType type);
