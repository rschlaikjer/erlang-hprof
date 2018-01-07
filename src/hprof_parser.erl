-module(hprof_parser).
-behavior(gen_server).

-include("include/records.hrl").

% Public API
-export([
    parse_file/1,
    parse_binary/1,
    close/1
]).

% gen_server
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    header :: #hprof_header{},
    records :: [any()]
}).

%% Public API

parse_file(Filename) ->
    gen_server:start_link(?MODULE, [{file, Filename}], []).

parse_binary(Binary) when is_binary(Binary) ->
    gen_server:start_link(?MODULE, [{binary, Binary}], []).

close(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, close).

%% Callbacks

init([{file, Filename}]) ->
    gen_server:cast(self(), {parse_file, Filename}),
    {ok, #state{}};
init([{binary, Binary}]) ->
    gen_server:cast(self(), {parse_binary, Binary}),
    {ok, #state{}}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({parse_file, Filename}, State) ->
    State1 = parse_file(State, Filename),
    {noreply, State1};
handle_cast({parse_binary, Binary}, State) ->
    State1 = parse_binary(State, Binary),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Implementation

parse_file(State, Filename) ->
    {ok, Bin} = file:read_file(Filename),
    parse_binary(State, Bin).

parse_binary(State, Bin) ->
    {Header, RecordsBinary} = parse_header(Bin),
    io:format("Header: ~p~n", [Header]),
    Records = parse_records(Header, RecordsBinary),
    State#state{
        header=Header,
        records=Records
    }.

parse_header(Bindata) ->
    % Header has the format:
    % Fixed header (JAVA PROFILE 1.0.3)
    % u32: Pointer size (in bytes)
    % u64: Time this dump was taken (milliseconds)
    <<?HPROF_HEADER_MAGIC, Bin1/binary>> = Bindata,
    <<HeapRefSize:?UINT32,
      DumpTimeMs:?UINT64,
      Rest/binary >> = Bin1,
    Header = #hprof_header{
        heap_ref_size = HeapRefSize,
        dump_timestamp_ms = DumpTimeMs
    },
    {Header, Rest}.

parse_records(Header=#hprof_header{}, Binary) when is_binary(Binary) ->
    parse_records(Header, Binary, []).

parse_records(_Header, <<>>, Accumulator) ->
    lists:reverse(Accumulator);
parse_records(Header, Binary, Acc) ->
    % Each record has the format:
    % u8: Record type
    % u32: Microseconds since header timestamp
    % u32: Size of this record (not including this header)
    % [u8] data
    <<RecordType:?UINT8,
      Microseconds:?UINT32,
      RecordSize:?UINT32,
      Rest/binary>> = Binary,
    <<RecordData:RecordSize/binary, Rest1/binary>> = Rest,
    RawRecord = #hprof_record_raw{
        record_type=RecordType,
        offset_microseconds=Microseconds,
        data_size=RecordSize,
        raw_data=RecordData
    },
    Record = parse_record(
        Header#hprof_header.heap_ref_size,
        RawRecord
    ),
    io:format("Parsed: ~p~n", [Record]),
    parse_records(Header, Rest1, [Record|Acc]).

parse_record(RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_STRING}) ->
    % Contains an ordinary utf-8 string
    <<Id:RefSize/big-unsigned-integer-unit:8,
      Data/binary>> = Raw#hprof_record_raw.raw_data,
    #hprof_record_string{
        id=Id,
        data=Data
    };
parse_record(RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_LOAD_CLASS}) ->
    % Loading a class
    % u32: Serial number
    % Ref: Class object ID
    % u32: Stack trace serial number
    % Ref: Class name string ID
    <<Serial:?UINT32,
      ClassObjId:RefSize/big-unsigned-integer-unit:8,
      StackSerial:?UINT32,
      ClassNameId:RefSize/big-unsigned-integer-unit:8
    >> = Raw#hprof_record_raw.raw_data,
    #hprof_record_load_class{
        serial=Serial,
        class_object_id=ClassObjId,
        stack_trace_serial=StackSerial,
        class_name_string_id=ClassNameId
    };
parse_record(_RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_UNLOAD_CLASS}) ->
    % Loading a class
    % u32: Serial number
    <<Serial:?UINT32
    >> = Raw#hprof_record_raw.raw_data,
    #hprof_record_unload_class{
        serial=Serial
    };
parse_record(RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_STACK_FRAME}) ->
    % Stack frame
    % Ref: Stack fram ID
    % Ref: Method name string ID
    % Ref: Method signature string ID
    % Ref: Source file name string ID
    % u32: Class serial number
    % u32: Location
    <<FrameId:RefSize/big-unsigned-integer-unit:8,
      MethodNameStringId:RefSize/big-unsigned-integer-unit:8,
      MethodSigStringId:RefSize/big-unsigned-integer-unit:8,
      SourceFileStringId:RefSize/big-unsigned-integer-unit:8,
      ClassSerial:?UINT32,
      Location:?UINT32
    >> = Raw#hprof_record_raw.raw_data,
    #hprof_record_stack_frame{
        frame_id=FrameId,
        method_name_string_id=MethodNameStringId,
        method_signature_string_id=MethodSigStringId,
        source_file_string_id=SourceFileStringId,
        class_serial=ClassSerial,
        location=Location
    };
parse_record(RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_STACK_TRACE}) ->
    % Stack trace
    % u32: Stack trace serial
    % u32: Thread serial
    % u32: Number of frames
    % [Ref]: Stack fram IDs
    <<Serial:?UINT32,
      ThreadSerial:?UINT32,
      FrameCount:?UINT32,
      FrameIdsBin/binary
    >> = Raw#hprof_record_raw.raw_data,
    FrameIds = [
        FrameId || <<FrameId:RefSize/big-unsigned-integer-unit:8>>
        <= FrameIdsBin
    ],
    #hprof_record_stack_trace{
        serial=Serial,
        thread_serial=ThreadSerial,
        frame_count=FrameCount,
        frame_ids=FrameIds
    };
parse_record(RefSize, Raw=#hprof_record_raw{record_type=?HPROF_TAG_HEAP_DUMP_SEGMENT}) ->
    parse_heap_dump_segments(RefSize, Raw);
parse_record(_RefSize, #hprof_record_raw{record_type=Type}) ->
    throw({bad_record, {unknown_type, Type}}).

parse_heap_dump_segments(RefSize, #hprof_record_raw{raw_data=Bin}) ->
    Segments = parse_heap_dump_segments(RefSize, Bin, []),
    #hprof_record_heap_dump_segment{
        segments=Segments
    }.

parse_heap_dump_segments(_RefSize, <<>>, Acc) ->
    lists:reverse(Acc);
parse_heap_dump_segments(RefSize, Bin, Acc) ->
    {Record, Rest} = parse_heap_dump_segment(RefSize, Bin),
    parse_heap_dump_segments(RefSize, Rest, [Record|Acc]).

parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_UNKNOWN, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8, Rest/binary>> = Bin,
    Root = #hprof_heap_root_unknown{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_JNI_GLOBAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      JniGlobalRefId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_global{
        object_id=ObjectId,
        jni_global_ref_id=JniGlobalRefId
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_JNI_LOCAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_local{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_JAVA_FRAME, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_java_frame{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_NATIVE_STACK, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_native_stack{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_STICKY_CLASS, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_sticky_class{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_THREAD_BLOCK, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_thread_block{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_MONITOR_USED, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_monitor_used{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_THREAD_OBJECT, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      StackTraceSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_thread_object{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        stack_trace_serial=StackTraceSerial
    },
    {Root, Rest};
parse_heap_dump_segment(_RefSize, <<?HPROF_CLASS_DUMP, _Bin/binary>>) ->
    % Huge, will get back to this
    throw(not_implemented);
parse_heap_dump_segment(RefSize, <<?HPROF_INSTANCE_DUMP, Bin/binary>>) ->
    % Can't actually parse these until we have the class dump data
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ClassObjectId:RefSize/big-unsigned-integer-unit:8,
      DataSize:?UINT32,
      Rest/binary>> = Bin,
    <<Data:DataSize/binary, Rest1/binary>> = Rest,
    Instance = #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    },
    {Instance, Rest1};
parse_heap_dump_segment(RefSize, <<?HPROF_OBJECT_ARRAY_DUMP, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      ElementClassObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    DataSize = ElementCount * RefSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Rest,
    Elements = [
        Elem || <<Elem:RefSize/big-unsigned-integer-unit:8>>
        <= ArrayData
    ],
    Array = #hprof_object_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_class_object_id=ElementClassObjectId,
        elements=Elements
    },
    {Array, Rest1};
parse_heap_dump_segment(RefSize, <<?HPROF_PRIMITIVE_ARRAY_DUMP, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      DataType:?UINT8,
      Rest/binary>> = Bin,
    ElementSize = primitive_size(RefSize, DataType),
    DataSize = ElementCount * ElementSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Rest,
    Elements = [
        parse_primitive(DataType, Elem) || <<Elem:ElementSize/binary>>
        <= ArrayData
    ],
    Array = #hprof_primitive_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_type=DataType,
        elements=Elements
    },
    {Array, Rest1};
parse_heap_dump_segment(RefSize, <<?HPROF_HEAP_DUMP_INFO, Bin/binary>>) ->
    <<HeapType:?UINT32,
      HeapNameStringId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Info = #hprof_heap_dump_info{
        heap_type=HeapType,
        heap_type_string_id=HeapNameStringId
    },
    {Info, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_INTERNED_STRING, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_interned_string{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(_, <<?HPROF_ROOT_FINALIZING, _/binary>>) ->
    throw({obsolete_tag, hprof_root_finalizing});
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_DEBUGGER, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_debugger{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(_, <<?HPROF_ROOT_REFERENCE_CLEANUP, _/binary>>) ->
    throw({obsolete_tag, hprof_root_reference_cleanup});
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_VM_INTERNAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_vm_internal{
        object_id=ObjectId
    },
    {Root, Rest};
parse_heap_dump_segment(RefSize, <<?HPROF_ROOT_JNI_MONITOR, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_monitor{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    {Root, Rest};
parse_heap_dump_segment(_, <<?HPROF_UNREACHABLE, _/binary>>) ->
    throw({obsolete_tag, hprof_root_unreachable});
parse_heap_dump_segment(_, <<?HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, _/binary>>) ->
    throw({obsolete_tag, hprof_primitive_array_nodata_dump}).

primitive_size(RefSize, ?HPROF_BASIC_OBJECT) -> RefSize;
primitive_size(_, ?HPROF_BASIC_BOOLEAN) -> 1;
primitive_size(_, ?HPROF_BASIC_CHAR) -> 2;
primitive_size(_, ?HPROF_BASIC_FLOAT) -> 4;
primitive_size(_, ?HPROF_BASIC_DOUBLE) -> 8;
primitive_size(_, ?HPROF_BASIC_BYTE) -> 1;
primitive_size(_, ?HPROF_BASIC_SHORT) -> 2;
primitive_size(_, ?HPROF_BASIC_INT) -> 4;
primitive_size(_, ?HPROF_BASIC_LONG) -> 8.

parse_primitive(?HPROF_BASIC_OBJECT, <<V:?UINT32>>) -> V;
parse_primitive(?HPROF_BASIC_OBJECT, <<V:?UINT64>>) -> V;
parse_primitive(?HPROF_BASIC_BOOLEAN, <<V:?UINT8>>) -> V;
parse_primitive(?HPROF_BASIC_CHAR, Bin) -> Bin;
parse_primitive(?HPROF_BASIC_FLOAT, <<V:32/float>>) -> V;
parse_primitive(?HPROF_BASIC_DOUBLE, <<V:64/float>>) -> V;
parse_primitive(?HPROF_BASIC_BYTE, <<V:?INT8>>) -> V;
parse_primitive(?HPROF_BASIC_SHORT, <<V:?INT16>>) -> V;
parse_primitive(?HPROF_BASIC_INT, <<V:?INT32>>) -> V;
parse_primitive(?HPROF_BASIC_LONG, <<V:?INT64>>) -> V.
