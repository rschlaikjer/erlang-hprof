# Scripts


Handful of somewhat specific functionality that might be useful, but not worth
including in the library itself.

Scripts can be built by `cd`ing into their directory and running
`rebar3 escriptize`, with the compiled script ending up in `_build/default/bin`.

### Static reference checker

If you suspect that an object isn't being garbage collected because it's held
through some series of pointers that ends in a static variable, this script
should come in handy. Given a dump file and an object ID, it will scan the
reference graph to try and find chains of pointers from a static context that
eventually reference your object.

As an example, if I had an activity that I suspected wasn't getting garbage
collected I could first find it's ID using the REPL:

    rr("include/records.hrl").
    {ok, Parser} = hprof_parser:parse_file(InFile).
    {ok, ActivityRef} = hprof_parser:get_instances_for_class(Parser, <<"com.example.activity.MyNonGcActivity">>).
    {ok, ObjectIds} = hprof:await_acc(ActivityRef, [], fun(A, I) -> [I#hprof_heap_instance.object_id|A] end).
    % ObjectIds: [901239520,896555536,896052368,895532976,...]

Once we know the ID is of an object that we think should have been GCd, we can
use the script to find out what's keeping references to it:

    $ _build/default/bin/main dump.hprof 896555536
    Populating reference mapping table...
     - Instances...
     - Object arrays...
     - Class dumps...
    Loaded 1805702 object mappings in 201 seconds
    Waiting for reference chains...
    Found statically referenced chain:
     com.example.utility.CardCache -static cachedCards-> 866149120
     java.lang.Object[] array (857898624) -> 896566080
     com.example.cards.ContentCard (896566080) -mParent-> 896566032
     com.example.cards.ContentCard$ViewHolder(895754848) -mParent-> 896571264
     com.example.activity.MyNonGcActivity$1 (896571264) -this$0-> 896555536
     com.example.activity.MyNonGcActivity (896555536)
