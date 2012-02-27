-module(gdrl_limiter).

-behaviour(gen_server).

%% API
-export([
    start_link/2,
    link/2,
    adjust/2,
    info/1,
    tick/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(RESPONSIVENESS(D), (1/(2*D))).
-define(ELSE, true).

-record(state, {
    id :: integer(),
    capacity :: float(),
    demand :: float(),
    links = [] :: [pid()]
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id, Capacity) ->
    gen_server:start_link(?MODULE, [Id, Capacity], []).

link(Ref, Node) ->
    gen_server:call(Ref, {link, Node}).

info(Ref) ->
    gen_server:call(Ref, info).

%%%
% @doc represents a single timestep. During the tick
% we adjust our fill percentage to match other nodes in cluster
tick(Ref) ->
    gen_server:call(Ref, tick).

%%%
% @doc our goal is to get every performance measure equal. In this case
% were using (Capacity / Demand)
adjust(Ref, Q1) ->
    {ok, CapacityToAdd} = gen_server:call(Ref, {adjust, Q1}),
    CapacityToAdd.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Id, Capacity]) ->
    random:seed(now()),
    Demand = random:uniform(1000),
    io:format("[Limiter ~p] :: Initialize C: ~p F:~p~n", [Id, Capacity, Demand]),
    {ok, #state{id=Id, capacity=Capacity, demand=Demand}}.

handle_call(info, _From, #state{id=Id, demand=Demand, capacity=Capacity} = State) ->
    {reply, [Id, Demand, Capacity], State};

handle_call(tick, _From, #state{demand=Demand, capacity=Capacity, links=Links} = State) ->
    Q1 = performance(Demand, Capacity),

    if Q1 == infinity ->
        {reply, ok, State};
    ?ELSE ->
        % iterate over neighbours, find
        % one with a fill ratio thats higher
        NewCap = lists:foldl(
            fun(Ref, Accum) ->
                Accum + ?MODULE:adjust(Ref, Q1)
            end,
            Capacity,
            Links
        ),
        {reply, ok, State#state{capacity=NewCap}}
    end;

handle_call({adjust, Q1}, _From, #state{demand=Demand, capacity=Capacity, links=Links} = State) ->
    Q2 = performance(Demand, Capacity),
    Degree = length(Links),

    Multiplier = case Q2 of
        infinity ->
            % demand at this limiter is 0 but it has
            % capacity, get rid of it ASAP take 100%
            ?RESPONSIVENESS(Degree) * 1;
        _ when Q1 < Q2 ->
            % we take the min(1, X) here because
            % you can have a limiter with a fill percentage over 100
            % ie. when demand is higher than capacity
            ?RESPONSIVENESS(Degree) * (Q2 - Q1);
        _ ->
            % the fill ratio C/D at this limiter
            % is higher than the other
            0
    end,

    % we need to truncate our floating point number here
    % otherwise will start getting some pretty big numbers in memory
    TakeAway = trunc_float(Multiplier * Demand),

    {reply, {ok, TakeAway}, State#state{capacity=Capacity-TakeAway}};

handle_call({link, Node}, _From, #state{links=Links} = State) ->
    io:format("[Limiter ~p] :: Linked with ~p~n", [State#state.id, Node]),
    {reply, ok, State#state{links=Links ++ [Node]}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

performance(Demand, Capacity) ->
    case Demand of
        0 -> infinity;
        _ -> Capacity / Demand
    end.

trunc_float(0) -> trunc_float(0.0);
trunc_float(undefined) -> undefined;
trunc_float(F) ->
    [S] = io_lib:format("~.4f",[F]),
    list_to_float(S).
