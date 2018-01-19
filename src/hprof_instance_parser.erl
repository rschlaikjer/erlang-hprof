-module(hprof_instance_parser).

-include("include/records.hrl").

% Public API
-export([
    parse_instances/6
]).

-record(state, {
    parser :: pid(),
    recipient :: pid(),
    ref :: reference(),
    accept_fun :: function(),
    ref_size :: 4 | 8,
    ets_class_dump :: reference()
}).

%% Public

parse_instances(Parser, Recipient, Ref, AcceptFun, Binary, ClassEts) ->
    State = #state{
        parser = Parser,
        recipient = Recipient,
        ref = Ref,
        accept_fun = AcceptFun,
        ets_class_dump = ClassEts
    },
    parse_instances_header(State, Binary).

%% Internal

parse_instances_header(State, Binary) ->
    % Peel off and ignore the header, with the exception of the heap ref size
    <<?HPROF_HEADER_MAGIC,
      RefSize:?UINT32,
      _DumpTimeMs:?UINT64,
      Rest/binary >> = Binary,
    parse_instances_segments(State#state{ref_size=RefSize}, Rest).

parse_instances_segments(State, <<>>) ->
    State#state.recipient ! {hprof_parser, State#state.ref, ok};
parse_instances_segments(State, <<Binary/binary>>) ->
    <<RecordType:?UINT8,
      _Microseconds:?UINT32,
      RecordSize:?UINT32,
      Rest/binary>> = Binary,
    parse_instances_segment(State, Rest, RecordSize, RecordType).

% Only care about heap dump segments here
parse_instances_segment(State, <<Binary/binary>>, RecordSize, ?HPROF_TAG_HEAP_DUMP_SEGMENT) ->
    % For heap segments, delve into the segment parsing code.
    parse_instances_heap_dump(State, Binary, RecordSize);
parse_instances_segment(State, <<Binary/binary>>, RecordSize, _RecordType) ->
    % Other segments, just skip over the data.
    <<_Record:RecordSize/binary, Rest/binary>> = Binary,
    parse_instances_segments(State, Rest).

parse_instances_heap_dump(State, <<Binary/binary>>, 0) ->
    % This segment is exhausted, jump back to the segment handler
    parse_instances_segments(State, Binary);
parse_instances_heap_dump(State, <<?HPROF_INSTANCE_DUMP, Bin/binary>>, RemainingBytes) ->
    % The only class we actually care about here.
    % Grab all the fields
    RefSize = State#state.ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ClassObjectId:RefSize/big-unsigned-integer-unit:8,
      DataSize:?UINT32,
      Rest/binary>> = Bin,
    <<Data:DataSize/binary, Rest1/binary>> = Rest,
    BytesRead = 1 + RefSize + 4 + RefSize + 4 + DataSize,

    % Parse out the instance data
    Instance = parse_instance_record(State, #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    }),

    % Check if the instance satisfies our acceptance function
    AcceptFun = State#state.accept_fun,
    case AcceptFun(Instance) of
        false ->
            % If it doesn't, ignore it
            ok;
        true ->
            % If it does, chuck it over to the recipient.
            State#state.recipient ! {hprof_parser, State#state.ref, Instance}
    end,
    parse_instances_heap_dump(State, Rest1, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_UNKNOWN, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_JNI_GLOBAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _JniGlobalRefId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + (RefSize * 2),
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_JNI_LOCAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      _FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_JAVA_FRAME, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      _FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_NATIVE_STACK, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 4,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_STICKY_CLASS, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_THREAD_BLOCK, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 4,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_MONITOR_USED, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_THREAD_OBJECT, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      _StackTraceSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_CLASS_DUMP, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ClassObjId:RefSize/big-unsigned-integer-unit:8,
      _StackTraceSerial:?UINT32,
      _SuperClassObjId:RefSize/big-unsigned-integer-unit:8,
      _ClassLoaderObjId:RefSize/big-unsigned-integer-unit:8,
      _Signer:RefSize/big-unsigned-integer-unit:8,
      _ProtDomain:RefSize/big-unsigned-integer-unit:8,
      _Reserved1:RefSize/big-unsigned-integer-unit:8,
      _Reserved2:RefSize/big-unsigned-integer-unit:8,
      _InstanceSize:?UINT32,
      NumConstants:?UINT16,
      Rest1/binary>> = Bin,
    HeaderBytesRead = RefSize + 4 + RefSize * 6 + 4 + 2 + 1,

    % Constants
    {ConstantBytes, Rest2} = skip_constants(State, Rest1, NumConstants),

    % Statics
    <<NumStatics:?UINT16, Rest3/binary>> = Rest2,
    {StaticBytes, Rest4} = skip_statics(State, Rest3, NumStatics),

    % Instance vars
    <<NumInstances:?UINT16, Rest5/binary>> = Rest4,
    {InstanceBytes, Rest6} = skip_instances(State, Rest5, NumInstances),

    BytesLeft = RemainingBytes - (
        HeaderBytesRead + ConstantBytes + 2 + StaticBytes + 2 + InstanceBytes
    ),
    parse_instances_heap_dump(State, Rest6, BytesLeft);
parse_instances_heap_dump(State, <<?HPROF_OBJECT_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      _ElementClassObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    DataSize = ElementCount * RefSize,
    <<_ArrayData:DataSize/binary, Rest1/binary>> = Rest,
    BytesRead = 1 + RefSize + 8 + RefSize + DataSize,
    parse_instances_heap_dump(State, Rest1, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_PRIMITIVE_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      DataType:?UINT8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 9,
    RefSize = State#state.ref_size,
    ElementSize = primitive_size(RefSize, DataType),
    ArraySize = ElementSize * ElementCount,
    <<_ArrayData:ArraySize/binary, Rest1/binary>> = Rest,
    parse_instances_heap_dump(State, Rest1, RemainingBytes - (BytesRead + ArraySize));
parse_instances_heap_dump(State, <<?HPROF_HEAP_DUMP_INFO, Bin/binary>>, RemainingBytes) ->
    % Not currently tracking which heap things are in
    RefSize = State#state.ref_size,
    <<_HeapType:?UINT32,
      _HeapNameStringId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + 4 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_INTERNED_STRING, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(_State, <<?HPROF_ROOT_FINALIZING, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_finalizing});
parse_instances_heap_dump(State, <<?HPROF_ROOT_DEBUGGER, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(_State, <<?HPROF_ROOT_REFERENCE_CLEANUP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_reference_cleanup});
parse_instances_heap_dump(State, <<?HPROF_ROOT_VM_INTERNAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(State, <<?HPROF_ROOT_JNI_MONITOR, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _ThreadSerial:?UINT32,
      _FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    parse_instances_heap_dump(State, Rest, RemainingBytes - BytesRead);
parse_instances_heap_dump(_State, <<?HPROF_UNREACHABLE, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_unreachable});
parse_instances_heap_dump(_State, <<?HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_primitive_array_nodata_dump}).

skip_constants(State, <<Binary/binary>>, NumConstants) ->
    skip_constants(State, Binary, NumConstants, 0).
skip_constants(_State, <<Binary/binary>>, 0, BytesRead) ->
    {BytesRead, Binary};
skip_constants(State, <<Binary/binary>>, NumConstants, BytesRead) ->
    <<_ConstantPoolIndex:?UINT16,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(State#state.ref_size, Type),
    <<_FieldDataBin:FieldSize/binary, Rest1/binary>> = Rest,
    skip_constants(State, Rest1, NumConstants - 1, BytesRead + FieldSize + 3).

skip_statics(State, <<Binary/binary>>, NumStatics) ->
    skip_statics(State, Binary, NumStatics, 0).
skip_statics(_State, <<Binary/binary>>, 0, BytesRead) ->
    {BytesRead, Binary};
skip_statics(State, <<Binary/binary>>, NumStatics, BytesRead) ->
    RefSize = State#state.ref_size,
    <<_FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(RefSize, Type),
    <<_FieldDataBin:FieldSize/binary, Rest1/binary>> = Rest,
    skip_statics(State, Rest1, NumStatics-1, BytesRead + 1 + RefSize + FieldSize).

skip_instances(State, <<Binary/binary>>, NumFields) ->
    skip_instances(State, Binary, NumFields, 0).
skip_instances(_State, <<Binary/binary>>, 0, BytesRead) ->
    {BytesRead, Binary};
skip_instances(State, <<Binary/binary>>, NumFields, BytesRead) ->
    RefSize = State#state.ref_size,
    <<_FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      _Type:?UINT8,
      Rest/binary
    >> = Binary,
    skip_instances(State, Rest, NumFields - 1, BytesRead + RefSize + 1).

parse_instance_record(State, R=#hprof_heap_instance_raw{}) ->
    % Break out the record data
    #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    } = R,

    % Parse the class data
    Values = parse_instance_record_for_class(
        State, Data, ClassObjectId, #{}
    ),
    #hprof_heap_instance{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        instance_values=Values
    }.

% Class ID of 0 is 'Object'
parse_instance_record_for_class(_State, <<_Binary/binary>>, 0, Acc) ->
    Acc;
parse_instance_record_for_class(State, <<Binary/binary>>, ClassId, Acc) ->
    % Get the class instance for this object
    ClassObj = hd(ets:lookup(State#state.ets_class_dump, ClassId)),

    % Get the list of instance fields to read data for, and extract them
    ClassInstanceFields = ClassObj#hprof_class_dump.instance_fields,
    SuperClassId = ClassObj#hprof_class_dump.superclass_object,
    extract_instance_id_fields(
        State, Binary, SuperClassId, ClassInstanceFields, Acc
    ).

extract_instance_id_fields(State, <<Binary/binary>>, SuperClassId, [], Acc) ->
    parse_instance_record_for_class(
        State, Binary, SuperClassId, Acc
    );
extract_instance_id_fields(State, <<Binary/binary>>, SuperClassId, [Field|Fields], Acc) ->
    #hprof_instance_field{name_string_id=Name, type=Type} = Field,
    RefSize = State#state.ref_size,
    DataSize = primitive_size(RefSize, Type),
    extract_instance_id_field(
        State, Binary, SuperClassId, Fields, Acc, Name, Type, DataSize
    ).

extract_instance_id_field(State, <<Binary/binary>>, SuperClassId, Fields, Acc, Name, Type, DataSize) ->
    <<PrimitiveBin:DataSize/binary, Rest/binary>> = Binary,
    Value = #hprof_instance_field{
        name_string_id=Name,
        type=Type,
        value=parse_primitive(PrimitiveBin, Type)
    },
    extract_instance_id_fields(
        State, Rest, SuperClassId, Fields, maps:put(Name, Value, Acc)
    ).

primitive_size(RefSize, ?HPROF_BASIC_OBJECT) -> RefSize;
primitive_size(_, ?HPROF_BASIC_BOOLEAN) -> 1;
primitive_size(_, ?HPROF_BASIC_CHAR) -> 2;
primitive_size(_, ?HPROF_BASIC_FLOAT) -> 4;
primitive_size(_, ?HPROF_BASIC_DOUBLE) -> 8;
primitive_size(_, ?HPROF_BASIC_BYTE) -> 1;
primitive_size(_, ?HPROF_BASIC_SHORT) -> 2;
primitive_size(_, ?HPROF_BASIC_INT) -> 4;
primitive_size(_, ?HPROF_BASIC_LONG) -> 8.

parse_primitive(<<V:?UINT32>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT64>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT8>>, ?HPROF_BASIC_BOOLEAN) -> V;
parse_primitive(<<AH:1/binary, AL:1/binary>>, ?HPROF_BASIC_CHAR) -> <<AH/binary, AL/binary>>;
parse_primitive(<<255, 128, 0, 0>>, ?HPROF_BASIC_FLOAT) -> minus_infinity;
parse_primitive(<<127, 128, 0, 0>>, ?HPROF_BASIC_FLOAT) -> infinity;
parse_primitive(<<127, 192, 0, 0>>, ?HPROF_BASIC_FLOAT) -> nan;
parse_primitive(<<V:32/float>>, ?HPROF_BASIC_FLOAT) -> V;
parse_primitive(<<255,240,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> minus_infinity;
parse_primitive(<<127,248,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> infinity;
parse_primitive(<<127,240,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> nan;
parse_primitive(<<V:64/float>>, ?HPROF_BASIC_DOUBLE) -> V;
parse_primitive(<<V:?INT8>>, ?HPROF_BASIC_BYTE) -> V;
parse_primitive(<<V:?INT16>>, ?HPROF_BASIC_SHORT) -> V;
parse_primitive(<<V:?INT32>>, ?HPROF_BASIC_INT) -> V;
parse_primitive(<<V:?INT64>>, ?HPROF_BASIC_LONG) -> V.

parse_class_dump_segment_optimized(State, <<Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<ClassObjId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      SuperClassObjId:RefSize/big-unsigned-integer-unit:8,
      ClassLoaderObjId:RefSize/big-unsigned-integer-unit:8,
      Signer:RefSize/big-unsigned-integer-unit:8,
      ProtDomain:RefSize/big-unsigned-integer-unit:8,
      _Reserved1:RefSize/big-unsigned-integer-unit:8,
      _Reserved2:RefSize/big-unsigned-integer-unit:8,
      InstanceSize:?UINT32,
      NumConstants:?UINT16,
      Rest1/binary>> = Bin,

    HeaderBytesRead = RefSize + 4 + RefSize * 6 + 4 + 2,

    % Start building the class
    Class = #hprof_class_dump{
        class_object=ClassObjId,
        stack_trace_serial=StackTraceSerial,
        superclass_object=SuperClassObjId,
        classloader_object=ClassLoaderObjId,
        signer=Signer,
        prot_domain=ProtDomain,
        instance_size=InstanceSize,
        num_constants=NumConstants
    },

    % Empty constant pool for ART, apparently, but may as well be complete
    % about it.
    parse_class_dump_constants(
        State, Rest1, Class, RemainingBytes-HeaderBytesRead
    ).

parse_class_dump_constants(State, <<Binary/binary>>, Class=#hprof_class_dump{num_constants=NC}, RemainingBytes) ->
    parse_class_dump_constants(State, Binary, Class, NC, [], RemainingBytes).

parse_class_dump_constants(State, <<Binary/binary>>, Class, 0, Acc, RemainingBytes) ->
    parse_class_dump_segment_optimized_statics(
        State, Binary, Class#hprof_class_dump{constants=lists:reverse(Acc)}, RemainingBytes
    );
parse_class_dump_constants(State, <<Binary/binary>>, Class, NumConstants, Acc, RemainingBytes) ->
    <<ConstantPoolIndex:?UINT16,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    RefSize = State#state.ref_size,
    FieldSize = primitive_size(RefSize, Type),
    parse_class_dump_constant(
        State, Rest, Class, NumConstants, Acc,
        RemainingBytes, ConstantPoolIndex, Type, FieldSize
    ).

parse_class_dump_constant(State, <<Binary/binary>>, Class, NumConstants, Acc,
                          RemainingBytes, ConstantPoolIndex, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_constant_field{
        constant_pool_index=ConstantPoolIndex,
        type=Type,
        data=FieldData
    },
    parse_class_dump_constants(
        State, Rest, Class, NumConstants-1, [Field|Acc],
        RemainingBytes - (2 + 1 + FieldSize)
    ).

parse_class_dump_segment_optimized_statics(State, <<Bin/binary>>, Class, RemainingBytes) ->
    %     static_fields=Statics,
    %     num_instance_fields=NumInstances,
    %     instance_fields=Instances
    % Static fields
    <<NumStatics:?UINT16, Rest/binary>> = Bin,
    parse_class_dump_static_fields(
        State, Rest, Class#hprof_class_dump{
            num_static_fields=NumStatics
        }, RemainingBytes - 2
    ).

parse_class_dump_static_fields(State, <<Binary/binary>>, Class=#hprof_class_dump{num_static_fields=NS}, RemainingBytes) ->
    parse_class_dump_static_fields(State, Binary, NS, [], Class, RemainingBytes).
parse_class_dump_static_fields(State, <<Binary/binary>>, 0, Acc, Class, RemainingBytes) ->
    parse_class_dump_segment_optimized_instances(
        State, Binary,
        Class#hprof_class_dump{static_fields=lists:reverse(Acc)},
        RemainingBytes
    );
parse_class_dump_static_fields(State, <<Binary/binary>>, NumStatics, Acc, Class, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(RefSize, Type),
    parse_class_dump_static(
        State, Rest, NumStatics, Acc, Class, RemainingBytes - (RefSize + 1),
        FieldNameStringId, Type, FieldSize
    ).

parse_class_dump_static(State, <<Binary/binary>>, NumStatics, Acc, Class, RemainingBytes, FieldNameStringId, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_static_field{
        name_string_id=FieldNameStringId,
        type=Type,
        data=FieldData
    },
    parse_class_dump_static_fields(
        State, Rest, NumStatics-1, [Field|Acc], Class,
        RemainingBytes - FieldSize
    ).

parse_class_dump_segment_optimized_instances(State, <<Bin/binary>>, Class, RemainingBytes) ->
    % Instance Fields
    <<NumInstances:?UINT16, Rest/binary>> = Bin,
    parse_class_dump_instance_fields(
        State, Rest, Class#hprof_class_dump{
            num_instance_fields=NumInstances
        }, RemainingBytes - 2
    ).

parse_class_dump_instance_fields(State, <<Binary/binary>>,
                                 Class=#hprof_class_dump{num_instance_fields=N},
                                 RemainingBytes) ->
    parse_class_dump_instance_fields(
        State, Binary, N, [], Class, RemainingBytes
    ).

parse_class_dump_instance_fields(State, <<Binary/binary>>, 0, Acc, Class, RemainingBytes) ->
    parse_class_dump_segment_finalize(
        State, Binary, Class#hprof_class_dump{
            instance_fields=lists:reverse(Acc)
        }, RemainingBytes
    );
parse_class_dump_instance_fields(State, <<Binary/binary>>, NumFields, Acc, Class, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    Field = #hprof_instance_field{
        name_string_id=FieldNameStringId,
        type=Type
    },
    parse_class_dump_instance_fields(
        State, Rest, NumFields-1, [Field|Acc],
        Class, RemainingBytes - (RefSize + 1)
    ).

parse_class_dump_segment_finalize(State, <<Bin/binary>>, Class, RemainingBytes) ->
    ets:insert(State#state.ets_class_dump, Class),
    parse_instances_heap_dump(State, Bin, RemainingBytes).
