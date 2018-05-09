-module(hprof).
-include_lib("hprof/include/records.hrl").

-export([
    await/2,
    await_acc/3,
    primitive_size/2,
    parse_primitive/2,
    primitive_name/1,
    primitive_ordinal/1
]).

% Helper method for receiving instance records as they are parsed, if using
% the streaming form of instance parsing.
% As the records are received, they will be passed to Fun.
% This method will not return until the parser is finished sending objects.
-spec await(reference(), function()) -> ok | {error, any()}.
await(Ref, Fun) ->
    receive
        {hprof_parser, Ref, {error, Reason}} ->
            {error, Reason};
        {hprof_parser, Ref, ok} ->
            ok;
        {hprof_parser, Ref, I} ->
            Fun(I),
            await(Ref, Fun)
    end.

% Like await_instances, but acts more like fold. An initial state can be passed,
% and will be passed as argument one to the lambda. The result of the lambda will
% then be the initial state for the next call, etc.
-spec await_acc(reference(), any(), function()) -> {ok, any()} | {error, any()}.
await_acc(Ref, Initial, Fun) ->
    receive
        {hprof_parser, Ref, {error, Reason}} ->
            {error, Reason};
        {hprof_parser, Ref, ok} ->
            {ok, Initial};
        {hprof_parser, Ref, I} ->
            await_acc(Ref, Fun(Initial, I), Fun)
    end.

% Get the size in bytes of a primitive type.
% One type (object pointer) depends on the heap reference size
% (whether the system is 32- or 64-bit)
-spec primitive_size(4 | 8, pos_integer()) -> pos_integer().
primitive_size(RefSize, Atom) when is_atom(Atom) ->
    primitive_size(RefSize, primitive_ordinal(Atom));
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
parse_primitive(<<B/binary>>, Atom) when is_atom(Atom) ->
    parse_primitive(B, primitive_ordinal(Atom));
parse_primitive(<<V:?UINT32>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT64>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT8>>, ?HPROF_BASIC_BOOLEAN) -> V > 0;
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

% Primitive type atom to number
primitive_ordinal(object) -> ?HPROF_BASIC_OBJECT;
primitive_ordinal(boolean) -> ?HPROF_BASIC_BOOLEAN;
primitive_ordinal(char) -> ?HPROF_BASIC_CHAR;
primitive_ordinal(float) -> ?HPROF_BASIC_FLOAT;
primitive_ordinal(double) -> ?HPROF_BASIC_DOUBLE;
primitive_ordinal(byte) -> ?HPROF_BASIC_BYTE;
primitive_ordinal(short) -> ?HPROF_BASIC_SHORT;
primitive_ordinal(int) -> ?HPROF_BASIC_INT;
primitive_ordinal(long) -> ?HPROF_BASIC_LONG.
