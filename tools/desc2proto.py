import sys
from google.protobuf import descriptor_pb2 as d

SCALARS = {
    d.FieldDescriptorProto.TYPE_DOUBLE: "double",
    d.FieldDescriptorProto.TYPE_FLOAT: "float",
    d.FieldDescriptorProto.TYPE_INT64: "int64",
    d.FieldDescriptorProto.TYPE_UINT64: "uint64",
    d.FieldDescriptorProto.TYPE_INT32: "int32",
    d.FieldDescriptorProto.TYPE_FIXED64: "fixed64",
    d.FieldDescriptorProto.TYPE_FIXED32: "fixed32",
    d.FieldDescriptorProto.TYPE_BOOL: "bool",
    d.FieldDescriptorProto.TYPE_STRING: "string",
    d.FieldDescriptorProto.TYPE_GROUP: "group",
    d.FieldDescriptorProto.TYPE_BYTES: "bytes",
    d.FieldDescriptorProto.TYPE_UINT32: "uint32",
    d.FieldDescriptorProto.TYPE_SFIXED32: "sfixed32",
    d.FieldDescriptorProto.TYPE_SFIXED64: "sfixed64",
    d.FieldDescriptorProto.TYPE_SINT32: "sint32",
    d.FieldDescriptorProto.TYPE_SINT64: "sint64",
}
LABEL = {
    d.FieldDescriptorProto.LABEL_OPTIONAL: "",
    d.FieldDescriptorProto.LABEL_REQUIRED: "required ",
    d.FieldDescriptorProto.LABEL_REPEATED: "repeated ",
}

out = []


def w(line=""):
    out.append(line)


def type_name(f, pkg):
    if f.type in (d.FieldDescriptorProto.TYPE_MESSAGE, d.FieldDescriptorProto.TYPE_ENUM):
        n = f.type_name
        if n.startswith("." + pkg + "."):
            return n[len(pkg) + 2:]
        if n.startswith("."):
            return n[1:]
        return n
    return SCALARS.get(f.type, "??")


def is_map_entry(m):
    return m.options.map_entry if m.HasField("options") else False


def map_kv(m):
    k = next(f for f in m.field if f.number == 1)
    v = next(f for f in m.field if f.number == 2)
    return k, v


def emit_default(f):
    # only emit non-default explicit defaults (proto2)
    if f.HasField("default_value"):
        if f.type == d.FieldDescriptorProto.TYPE_STRING:
            return f' [default = "{f.default_value}"]'
        return f" [default = {f.default_value}]"
    return ""


def emit_enum(e, pkg, indent):
    pad = "  " * indent
    w(f"{pad}enum {e.name} {{")
    for v in e.value:
        w(f"{pad}  {v.name} = {v.number};")
    w(f"{pad}}}")


def emit_message(m, pkg, indent):
    pad = "  " * indent
    w(f"{pad}message {m.name} {{")
    for e in m.enum_type:
        emit_enum(e, pkg, indent + 1)
        w()
    for n in m.nested_type:
        if is_map_entry(n):
            continue
        emit_message(n, pkg, indent + 1)
        w()

    oneofs = {o.name: [] for o in m.oneof_decl}
    for f in m.field:
        if f.HasField("oneof_index") and not f.proto3_optional:
            oneofs[m.oneof_decl[f.oneof_index].name].append(f)

    for f in m.field:
        if f.HasField("oneof_index") and not f.proto3_optional:
            continue
        if f.type == d.FieldDescriptorProto.TYPE_MESSAGE and f.label == d.FieldDescriptorProto.LABEL_REPEATED:
            try:
                nested = next(n for n in m.nested_type if n.name == f.type_name.split(".")[-1])
            except StopIteration:
                nested = None
            if nested is not None and is_map_entry(nested):
                k, v = map_kv(nested)
                w(f"{pad}  map<{type_name(k, pkg)}, {type_name(v, pkg)}> {f.name} = {f.number};")
                continue
        opt = "optional " if f.proto3_optional else ""
        w(f"{pad}  {opt}{LABEL[f.label]}{type_name(f, pkg)} {f.name} = {f.number}{emit_default(f)};")

    for name, fields in oneofs.items():
        if not fields:
            continue
        w(f"{pad}  oneof {name} {{")
        for f in fields:
            w(f"{pad}    {type_name(f, pkg)} {f.name} = {f.number};")
        w(f"{pad}  }}")

    for r in m.reserved_range:
        hi = r.end - 1
        w(f"{pad}  reserved {r.start}{'' if hi == r.start else ' to ' + (str(hi) if hi < 536870911 else 'max')};")
    for r in m.reserved_name:
        w(f'{pad}  reserved "{r}";')
    w(f"{pad}}}")


fd = d.FileDescriptorProto()
fd.ParseFromString(open(sys.argv[1], "rb").read())

w('syntax = "proto3";')
w()
w(f"package {fd.package};")
w()
for dep in fd.dependency:
    w(f'import "{dep}";')
w()
for o in fd.options.ListFields() if False else []:
    pass
w("service " + fd.service[0].name + " {")
def strip(n, pkg):
    n = n[1:] if n.startswith(".") else n
    return n[len(pkg) + 1:] if n.startswith(pkg + ".") else n


for m in fd.service[0].method:
    stream_in = "stream " if m.client_streaming else ""
    stream_out = "stream " if m.server_streaming else ""
    w(f"  rpc {m.name} ({stream_in}{strip(m.input_type, fd.package)}) returns ({stream_out}{strip(m.output_type, fd.package)});")
w("}")
w()
for e in fd.enum_type:
    emit_enum(e, fd.package, 0)
    w()
for m in fd.message_type:
    emit_message(m, fd.package, 0)
    w()

open(sys.argv[2], "w").write("\n".join(out) + "\n")
print("wrote", sys.argv[2], len(out), "lines")
