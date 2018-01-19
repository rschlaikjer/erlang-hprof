-module(hprof).
-include("include/records.hrl").

-export([
    await_instances/2,
    primitive_size/2,
    parse_primitive/2,
    primitive_name/1
]).

% Helper method for receiving instance records as they are parsed, if using
% the streaming form of instance parsing.
% As the records are received, they will be passed to Fun.
% This method will not return until the parser is finished sending objects.
-spec await_instances(reference(), function()) -> ok | {error, any()}.
await_instances(Ref, Fun) ->
    receive
        {hprof_parser, Ref, {error, Reason}} ->
            {error, Reason};
        {hprof_parser, Ref, ok} ->
            ok;
        {hprof_parser, Ref, I=#hprof_heap_instance{}} ->
            Fun(I),
            await_instances(Ref, Fun)
    end.

% Get the size in bytes of a primitive type.
% One type (object pointer) depends on the heap reference size
% (whether the system is 32- or 64-bit)
-spec primitive_size(4 | 8, pos_integer()) -> pos_integer().
primitive_size(RefSize, ?HPROF_BASIC_OBJECT) -> RefSize;
primitive_size(_, ?HPROF_BASIC_BOOLEAN) -> 1;
primitive_size(_, ?HPROF_BASIC_CHAR) -> 2;
primitive_size(_, ?HPROF_BASIC_FLOAT) -> 4;
primitive_size(_, ?HPROF_BASIC_DOUBLE) -> 8;
primitive_size(_, ?HPROF_BASIC_BYTE) -> 1;
primitive_size(_, ?HPROF_BASIC_SHORT) -> 2;
primitive_size(_, ?HPROF_BASIC_INT) -> 4;
primitive_size(_, ?HPROF_BASIC_LONG) -> 8.

% Parse the binary representation of a basic type to a reasonable
% erlang value.
% The only weird case is float/double - erlang doesn't support ±infinity
% or NaN, so we need to special case those.
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

% HProf type enum to atom name
-spec primitive_name(pos_integer()) -> atom().
primitive_name(?HPROF_BASIC_OBJECT) -> object;
primitive_name(?HPROF_BASIC_BOOLEAN) -> boolean;
primitive_name(?HPROF_BASIC_CHAR) -> char;
primitive_name(?HPROF_BASIC_FLOAT) -> float;
primitive_name(?HPROF_BASIC_DOUBLE) -> double;
primitive_name(?HPROF_BASIC_BYTE) -> byte;
primitive_name(?HPROF_BASIC_SHORT) -> short;
primitive_name(?HPROF_BASIC_INT) -> int;
primitive_name(?HPROF_BASIC_LONG) -> long.
