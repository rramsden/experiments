% GDRL (Generalized Distributed Rate Limiting) Algorithm
%
% This is a benchmark of the GDRL algorithm discussed in
% http://fourier.networks.imdea.org/people/~rade_stanojevic/GeneralizedDRL_IWQoS2009.pdf
%
% This is a simple token ring, each node has one neighbour
-module(gdrl).
-export([start/2]).

-define(MAXSTEPS, 100).
-define(MAPPINGS, [
    {1, [10,2]},
    {2, [1, 3]},
    {3, [2, 4]},
    {4, [3, 5]},
    {5, [4, 6]},
    {6, [5, 7]},
    {7, [6, 8]},
    {8, [7, 9]},
    {9, [8, 10]},
    {10,[9, 1]}
]).

%% ==============================================================
%% Public API
%% ==============================================================

%%
% @doc Let N be the number of servers
%      Let C be the aggregate service rate over entire cluster
start(N, C) ->
    Limiters = lists:map(fun({ok, Pid}) -> Pid end, [ gdrl_limiter:start_link(I, C/N) || I <- lists:seq(1, N) ]),
    [ build_links(I, Limiters) || I <- lists:seq(1, N) ],
    do_step(Limiters, ?MAXSTEPS).

%%%
% @doc calculate the fairness measure of the system
% this uses Jaines Fairness index to look
% at spare bandwidth at each node
fairness(Limiters) ->
    N = length(Limiters),
    Qs = lists:map(
        fun(Ref) ->
            [_, Flow, Capacity] = gdrl_limiter:info(Ref),
            Flow - Capacity
        end,
        Limiters
    ),
    X = lists:foldl(fun(Q, Accum) -> Q + Accum end, 0, Qs),
    Y = lists:foldl(fun(Q, Accum) -> Q*Q + Accum end, 0, Qs),
    (X*X) / (N*Y).

%% ==============================================================
%% Private API
%% ==============================================================
do_step(_, 0) -> ok;
do_step(Limiters, Step) ->
    lists:map(fun(L) -> gdrl_limiter:adjust(L) end, Limiters),
    io:format("C: ~p~n", [total_capacity(Limiters)]),
    io:format("System Fairness : ~p~n",[fairness(Limiters)]),
    do_step(Limiters, Step-1).

total_capacity(Limiters) ->
    lists:sum(
        lists:map(fun(L) ->
            [Id, Flow, Capacity] = gdrl_limiter:info(L),
            io:format("[Limiter ~p] ::  F: ~p   C: ~p~n",[Id, Flow, Capacity]),
            Capacity
        end, Limiters)
    ).

% this handy function will take an item in a list
% and add the next ?DEGREE items as neighbours
build_links(Index, Limiters) when is_list(Limiters) ->
    Ref = lists:nth(Index, Limiters),
    [V1, V2] = proplists:get_value(Index, ?MAPPINGS),
    gdrl_limiter:link(Ref, lists:nth(V1, Limiters)),
    gdrl_limiter:link(Ref, lists:nth(V2, Limiters)).

