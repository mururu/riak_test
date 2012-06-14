%% @private
-module(riak_test).
-export([main/1]).
-include_lib("eunit/include/eunit.hrl").

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

main(Args) ->
    [Config, Test | HarnessArgs]=Args,
    rt:load_config(Config),

    [add_deps(Dep) || Dep <- rt:config(rt_deps)],
    ENode = rt:config(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt:config(rt_cookie, riak),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),

    application:start(lager),
    LagerLevel = rt:config(rt_lager_level, debug),
    lager:set_loglevel(lager_console_backend, LagerLevel),

    %% rt:set_config(rtdev_path, Path),
    %% rt:set_config(rt_max_wait_time, 180000),
    %% rt:set_config(rt_retry_delay, 500),
    %% rt:set_config(rt_harness, rtbe),
    rt:setup_harness(Test, HarnessArgs),
    TestA = list_to_atom(Test),
    rt:set_config(rt_test, TestA),
    rt:if_coverage(fun rt:cover_compile/1, [TestA]),
    TestA = rt:config(rt_test),
    TestA:TestA(),
    rt:if_coverage(fun cover_analyze_file/1, [TestA]),
    rt:cleanup_harness(),
    ok.

cover_analyze_file(_TestMod) ->
    %% Modules = rt:cover_modules(TestMod),
    Modules = cover:modules(),
    lists:foreach(fun generate_coverage/1, Modules).

generate_coverage(Mod) ->
    {ok, File} = cover:analyze_to_file(Mod, [html]),
    lager:info("Wrote coverage file: ~p", [File]).
