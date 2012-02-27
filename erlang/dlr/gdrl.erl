-module(gdrl).
-export([start/2]).

-define(MAXSTEPS, 100000).

%% ==============================================================
%% Public API
%% ==============================================================

%%
% @doc Let N be the number of servers
%      Let C be the aggregate service rate over entire cluster
start(N, C) ->
    Limiters = lists:map(fun({ok, Pid}) -> Pid end, [ gdrl_limiter:start_link(I, C/N) || I <- lists:seq(1, N) ]),
    % keep it simple use a complete graph for now
    [ add_neighbours(I, Limiters) || I <- lists:seq(1, N) ],
    total_capacity(Limiters),
    do_tick(Limiters, ?MAXSTEPS),
    total_capacity(Limiters).

%%%
% @doc calculate the fairness measure of the system
% this uses Jaines Fairness index to look
% at spare bandwidth at each node
fairness(Limiters) ->
    N = length(Limiters),
    Qs = lists:map(
        fun(Ref) ->
            [_, Demand, Capacity] = gdrl_limiter:info(Ref),
            Capacity / Demand
        end,
        Limiters
    ),
    X = lists:foldl(fun(Q, Accum) -> Q + Accum end, 0, Qs),
    Y = lists:foldl(fun(Q, Accum) -> Q*Q + Accum end, 0, Qs),
    (X*X) / (N*Y).

%% ==============================================================
%% Private API
%% ==============================================================
do_tick(_, 0) -> ok;
do_tick(Limiters, Step) ->
    lists:map(fun(L) -> gdrl_limiter:tick(L) end, Limiters),
    io:format("System Fairness : ~p%~n",[trunc_float(fairness(Limiters) * 100)]),
    do_tick(Limiters, Step-1).

total_capacity(Limiters) ->
    lists:sum(
        lists:map(fun(L) ->
            [Id, Flow, Capacity] = gdrl_limiter:info(L),
            io:format("[Limiter ~p] ::  I: ~p   C: ~p~n",[Id, Flow, Capacity]),
            Capacity
        end, Limiters)
    ).

add_neighbours(Index, Limiters) when is_list(Limiters) ->
    Ref = lists:nth(Index, Limiters),
    Links = Limiters -- [Ref],
    lists:map(fun(Neighbour) ->
        gdrl_limiter:link(Ref, Neighbour)
    end, Links).

trunc_float(undefined) -> undefined;
trunc_float(F) ->
    [S] = io_lib:format("~.4f",[F]),
    list_to_float(S).
