-module(replication2_large_scale).
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%% @doc The large scale test is to test:
%%
%% Properties
%%
%% large scale repl (fullsync + realtime)
%% real time balance working
%% fullsync not halted by nodes up/down/add/remove
%%realtime not halted by nodes up/down/add/remove
%%
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-export([confirm/0]).

-record(state, {a_up = [], a_down = [], a_left = [], b_up= [], b_down= [], b_left =[]}).

-define(Conf,
        [{eleveldb, [
                     %% Required. Set to your data storage directory
                     {data_root, " /mnt"}
                    ]},
         {riak_kv, [{storage_backend, riak_kv_eleveldb_backend},
                    {anti_entropy, {on, []}},
                    {anti_entropy_build_limit, {100, 1000}},
                    {anti_entropy_concurrency, 100}
                   ]},
         {riak_repl, [{realtime_connection_rebalance_max_delay_secs, 10},
                      {fullsync_strategy, aae},
                      {fullsync_on_connect, false},
                      {fullsync_interval, disabled}
                     ]}
        ]).

-define(SizeA, 5).
-define(SizeB, 5).

-define(Sleep, 300 * 1000).

confirm() ->
    {ANodes, BNodes} = repl_util:deploy_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '<->'),
    State = #state{ a_up = ANodes, b_up = BNodes},
    
    AllLoadGens = rt_config:get(perf_loadgens, ["localhost"]),
    case length(AllLoadGens) of
        1 ->
            start_basho_bench(ANodes ++ BNodes, AllLoadGens);
        N ->
            {ALoadGens, BLoadGens} = lists:split(N div 2, AllLoadGens),
            start_basho_bench(ANodes, ALoadGens),
            start_basho_bench(BNodes, BLoadGens)
    end,

    timer:sleep(?Sleep),

    State1 = node_a_leave(State),
    timer:sleep(?Sleep),
    
    run_full_sync(State1),
    timer:sleep(?Sleep),


    State2 = node_a_down(State1),
    timer:sleep(?Sleep),

    State3 = node_b_down(State2),
    timer:sleep(?Sleep),

    State4 = node_a_up(State3),
    timer:sleep(?Sleep),
%%     run_full_sync(State4),
%%     timer:sleep(?Sleep),

    State5 = node_b_down(State4),
    timer:sleep(?Sleep),

    State6 = node_a_join(State5),
    timer:sleep(?Sleep),
    run_full_sync(State6),
    timer:sleep(?Sleep),

    State7 = node_b_up(State6),
    timer:sleep(?Sleep),
    run_full_sync(State7),
    rt_bench:stop_bench(),
    timer:sleep(?Sleep),
    run_full_sync(State7),

%%%  Functions for running random up/down of nodes.
%%     _ = random:seed(now()),
%%     State7 = lists:foldl(fun(_N, StateIn) ->
%%                                  NewState = random_action(StateIn),
%%                                  run_full_sync(NewState)
%%                          end, State, lists:seq(1,1000)),

    pass.

run_full_sync(State) ->
    lager:info("run_full_sync ~p -> ~p", [State#state.a_up, State#state.b_up]),
    LeaderA = prepare_cluster(State#state.a_up, State#state.b_up),
    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(State#state.a_up ++ State#state.b_up),
    {FullsyncTime, _} = timer:tc(repl_util,
                                  start_and_wait_until_fullsync_complete,
                                  [LeaderA]),
    lager:info("Fullsync time: ~p s", [FullsyncTime div 1000000]).

start_basho_bench(Nodes, LoadGens) ->
        PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, Nodes),

    LoadConfig = bacho_bench_config(PbIps),
    spawn(fun() -> rt_bench:bench(LoadConfig, Nodes, "50percentbackround", 1, false, LoadGens) end).

bacho_bench_config(HostList) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 30),
    BenchDuration =
        rt_config:get(basho_bench_duration, infinity),
    KeyGen =
        rt_config:get(basho_bench_keygen, {int_to_bin_bigendian, {pareto_int, 10000000}}),
    ValGen =
        rt_config:get(basho_bench_valgen, {exponential_bin, 1000, 10000}),
    Operations =
        rt_config:get(basho_bench_operations, [{get, 5},{put, 1}]),
    Bucket =
        rt_config:get(basho_bench_bucket, <<"testbucket">>),
    Driver =
        rt_config:get(basho_bench_driver, riakc_pb), 
    ReportInterval =
         rt_config:get(basho_bench_report_interval, 5),
    
    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList, 
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver,
                    ReportInterval).

random_action(State) ->
    [_|ValidAUp] = State#state.a_up,
    [_|ValidBUp] = State#state.b_up,
    NodeActionList =
        lists:flatten(
          [add_actions(ValidAUp, fun node_a_down/2),
           add_actions(ValidBUp, fun node_b_down/2),
           add_actions(State#state.a_down, fun node_a_up/2),
           add_actions(State#state.b_down, fun node_b_up/2)]),
    {Node, Action} = lists:nth(random:uniform(length(NodeActionList)), NodeActionList),
    Action(State, Node).

add_actions(Nodes, Action) ->
    [{Node, Action} || Node <- Nodes].

%%%%%%%% Start / Stop

node_a_down(State) ->
    node_a_down(State, lists:last(State#state.a_up)).
node_b_down(State) ->
    node_b_down(State, lists:last(State#state.b_up)).
node_a_up(State) ->
    node_a_up(State, lists:last(State#state.a_down)).
node_b_up(State) ->
    node_b_up(State, lists:last(State#state.b_down)).

node_a_down(State, Node) ->
    stop(Node),
    new_state(State, node_a_down, [Node]).
node_b_down(State, Node) ->
    stop(Node),
    new_state(State, node_b_down, [Node]).
node_a_up(State, Node) ->
    start(Node),
    new_state(State, node_a_up, [Node]).
node_b_up(State, Node) ->
    start(Node),
    new_state(State, node_b_up, [Node]).

stop(Node) ->
    rt:stop(Node),
    rt:wait_until_unpingable(Node),
    timer:sleep(5000),
    true.
start(Node) ->
    rt:start(Node),
    rt:wait_until_ready(Node),
    timer:sleep(5000),
    true.

%%%%%%%% Leave / Join
node_a_leave(State) ->
    node_a_leave(State, lists:last(State#state.a_up)).
node_b_leave(State) ->
    node_b_leave(State, lists:last(State#state.b_up)).
node_a_join(State) ->
    node_a_join(State, lists:last(State#state.a_left)).
node_b_join(State) ->
    node_b_join(State, lists:last(State#state.b_left)).

node_a_leave(State, Node) ->
    leave(Node),
    new_state(State, node_a_leave, [Node]).
node_b_leave(State, Node) ->
    leave(Node),
    new_state(State, node_b_leave, [Node]).
node_a_join(State, Node) ->
    join(Node, "A"),
    new_state(State, node_a_join, [Node]).
node_b_join(State, Node) ->
    join(Node, lists:first(State#state.b_up)),
    new_state(State, node_b_join, [Node]).

leave(Node) ->
    rt:leave(Node).
join(Node, Node1) ->
    rt:staged_join(Node, Node1),
    rt:plan_and_commit(Node1),
    rt:try_nodes_ready([Node], 3, 500).

%%%%%%%% Update state after action
new_state(S, node_a_down, Node) ->
    S#state{a_up = S#state.a_up -- Node,
            a_down = S#state.a_down ++ Node};
new_state(S, node_b_down, Node) ->
    S#state{b_up = S#state.b_up -- Node,
            b_down = S#state.b_down ++ Node};
new_state(S, node_a_up, Node) ->
    S#state{a_down = S#state.a_down -- Node,
            a_up = S#state.a_up ++ Node};
new_state(S, node_b_up, Node) ->
    S#state{b_down = S#state.b_down -- Node,
            b_up = S#state.b_up ++ Node};

new_state(S, node_a_leave, Node) ->
    S#state{a_up = S#state.a_up -- Node,
            a_left = S#state.a_left ++ Node};
new_state(S, node_b_leave, Node) ->
    S#state{b_up = S#state.b_up -- Node,
            b_left = S#state.b_left ++ Node};
new_state(S, node_a_join, Node) ->
    S#state{a_left = S#state.a_left -- Node,
            a_up = S#state.a_up ++ Node};
new_state(S, node_b_join, Node) ->
    S#state{b_left = S#state.b_left -- Node,
            b_up = S#state.b_up ++ Node}.


prepare_cluster([AFirst|_] = ANodes, [BFirst|_]) ->
    lager:info("Prepare cluster for fullsync"),
    LeaderA = rpc:call(AFirst,
                       riak_core_cluster_mgr, get_leader, []),
    {ok, {IP, Port}} = rpc:call(BFirst,
                                application, get_env, [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes), %% Only works when all nodes in ANodes are up.

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    LeaderA.
