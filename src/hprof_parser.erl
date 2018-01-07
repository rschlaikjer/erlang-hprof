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

-record(hprof_header, {
    heap_ref_size :: pos_integer(),
    dump_timestamp_ms :: pos_integer()
}).

-record(state, {
    header :: #hprof_header{},
    records :: [#hprof_record{}]
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
    {Header, RecordsBinary} = parse_fixed_header(Bin),
    Records = parse_records(RecordsBinary),
    State#state{
        header=Header,
        records=Records
    }.

parse_fixed_header(Bindata) ->
    <<?HPROF_HEADER_MAGIC, Bin1/binary>> = Bindata,
    <<HeapRefSize:4/integer-unit:8,
      DumpTimeMs:8/integer-unit:8,
      Rest/binary >> = Bin1,
    Header = #hprof_fixed_header{
        heap_ref_size = HeapRefSize,
        dump_timestamp_ms = DumpTimeMs
    },
    {Header, Rest}.

parse_records(Binary) when is_binary(Binary) ->
    parse_records(Binary, []).

parse_records(<<>>, Accumulator) ->
    lists:reverse(Accumulator);
parse_records(Binary, Acc) ->
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
    Record = parse_record(RecordType, Microseconds, RecordData),
    parse_records(Rest1, [Record|Acc]).

parse_record(RecordType, Microseconds, Data) ->
    ok.
