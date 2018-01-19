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
    parse_instances_class_dump(State, Bin, RemainingBytes - 1);
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
    ElementSize = hprof:primitive_size(RefSize, DataType),
    ArraySize = ElementSize * ElementCount,
    parse_instances_heap_skip_array(State, Rest, ArraySize, RemainingBytes - BytesRead);
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

% Skip over a primitive array
parse_instances_heap_skip_array(State, <<Bin/binary>>, ArraySize, RemainingBytes) ->
    <<_ArrayData:ArraySize/binary, Rest/binary>> = Bin,
    parse_instances_heap_dump(State, Rest, RemainingBytes - ArraySize).

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
    DataSize = hprof:primitive_size(RefSize, Type),
    extract_instance_id_field(
        State, Binary, SuperClassId, Fields, Acc, Name, Type, DataSize
    ).

extract_instance_id_field(State, <<Binary/binary>>, SuperClassId, Fields, Acc, Name, Type, DataSize) ->
    <<PrimitiveBin:DataSize/binary, Rest/binary>> = Binary,
    Value = #hprof_instance_field{
        name_string_id=Name,
        type=Type,
        value=hprof:parse_primitive(PrimitiveBin, Type)
    },
    extract_instance_id_fields(
        State, Rest, SuperClassId, Fields, maps:put(Name, Value, Acc)
    ).

parse_instances_class_dump(State, <<Bin/binary>>, RemainingBytes) ->
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

    HeaderBytesRead = RefSize + 4 + RefSize * 6 + 4 + 2,

    % Empty constant pool for ART, apparently, but may as well be complete
    % about it.
    skip_class_dump_constants(
        State, Rest1, NumConstants, RemainingBytes-HeaderBytesRead
    ).

skip_class_dump_constants(State, <<Binary/binary>>, 0, RemainingBytes) ->
    skip_class_dump_segment_optimized_statics(
        State, Binary, RemainingBytes
    );
skip_class_dump_constants(State, <<Binary/binary>>, NumConstants, RemainingBytes) ->
    <<_ConstantPoolIndex:?UINT16,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    RefSize = State#state.ref_size,
    FieldSize = hprof:primitive_size(RefSize, Type),
    skip_class_dump_constant(
        State, Rest, NumConstants,
        RemainingBytes, FieldSize
    ).

skip_class_dump_constant(State, <<Binary/binary>>, NumConstants,
                          RemainingBytes, FieldSize) ->
    <<_FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    skip_class_dump_constants(
        State, Rest, NumConstants-1,
        RemainingBytes - (2 + 1 + FieldSize)
    ).

skip_class_dump_segment_optimized_statics(State, <<Bin/binary>>, RemainingBytes) ->
    %     static_fields=Statics,
    %     num_instance_fields=NumInstances,
    %     instance_fields=Instances
    % Static fields
    <<NumStatics:?UINT16, Rest/binary>> = Bin,
    skip_class_dump_static_fields(
        State, Rest, NumStatics, RemainingBytes - 2
    ).

skip_class_dump_static_fields(State, <<Binary/binary>>, 0, RemainingBytes) ->
    skip_class_dump_segment_optimized_instances(
        State, Binary, RemainingBytes
    );
skip_class_dump_static_fields(State, <<Binary/binary>>, NumStatics, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = hprof:primitive_size(RefSize, Type),
    skip_class_dump_static(
        State, Rest, NumStatics, RemainingBytes - (RefSize + 1), FieldSize
    ).

skip_class_dump_static(State, <<Binary/binary>>, NumStatics, RemainingBytes, FieldSize) ->
    <<_FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    skip_class_dump_static_fields(
        State, Rest, NumStatics-1, RemainingBytes - FieldSize
    ).

skip_class_dump_segment_optimized_instances(State, <<Bin/binary>>, RemainingBytes) ->
    % Instance Fields
    <<NumInstances:?UINT16, Rest/binary>> = Bin,
    skip_class_dump_instance_fields(
        State, Rest, NumInstances, RemainingBytes - 2
    ).

skip_class_dump_instance_fields(State, <<Binary/binary>>, 0, RemainingBytes) ->
    skip_class_dump_segment_finalize(
        State, Binary, RemainingBytes
    );
skip_class_dump_instance_fields(State, <<Binary/binary>>, NumFields, RemainingBytes) ->
    RefSize = State#state.ref_size,
    <<_FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      _Type:?UINT8,
      Rest/binary
    >> = Binary,
    skip_class_dump_instance_fields(
        State, Rest, NumFields-1, RemainingBytes - (RefSize + 1)
    ).

skip_class_dump_segment_finalize(State, <<Bin/binary>>, RemainingBytes) ->
    parse_instances_heap_dump(State, Bin, RemainingBytes).
