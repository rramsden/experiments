-module(gdrl_limiter).

-behaviour(gen_server).

%% API
-export([
    start_link/2,
    link/2,
    info/1,
    adjust/1,
    spare_bandwidth/1
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

-record(state, {
    id :: integer(),
    capacity :: float(),
    flow :: float(),
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

adjust(Ref) ->
    gen_server:call(Ref, adjust).

spare_bandwidth(Ref) ->
    gen_server:call(Ref, spare_bandwidth).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Id, Capacity]) ->
    random:seed(now()),
    Flow = random:uniform(1000),
    io:format("[Limiter ~p] :: Initialize C: ~p F:~p~n",[Id, Capacity, Flow]),
    {ok, #state{id=Id, capacity=Capacity, flow=Flow}}.

handle_call(info, _From, #state{flow=Flow, capacity=Capacity} = State) ->
    {reply, [Flow, Capacity], State};

handle_call(adjust, _From, #state{flow=Flow, capacity=Capacity} = State) ->
    Degree = length(State#state.links),
    SpareBandwidth = Flow - Capacity,
    RefTotal = lists:map(fun({ok, Q}) -> Q end, [ spare_bandwidth(Ref) || Ref <- State#state.links ]),
    Delta = lists:sum(lists:map(fun(D) -> (SpareBandwidth - D) end, RefTotal)),
    NewCap = Capacity + (1/(2*Degree))*Delta,
    {reply, ok, State#state{capacity=NewCap}};

handle_call(spare_bandwidth, _From, #state{flow=Flow, capacity=Capacity} = State) ->
    SpareBandwidth = Flow - Capacity,
    {reply, {ok, SpareBandwidth}, State};

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
