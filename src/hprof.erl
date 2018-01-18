-module(hprof).
-include("include/records.hrl").

-export([
    await_instances/2
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
