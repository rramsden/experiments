-module(spsp).
-export([sum/0, average/0, add_peers/2]).

-record(node, {
    value,
    weight,
    peers = []
}).

sum() ->
    Pid0 = create({10, 1}),
    Pid1 = create({10, 0}), 
    Pid2 = create({10, 0}),
    add_peers(Pid0, [Pid1, Pid2]),
    add_peers(Pid1, [Pid0, Pid2]),
    add_peers(Pid2, [Pid0, Pid1]),

    start([Pid0, Pid1, Pid2]).

average() ->
    Pid0 = create({10, 1}),
    Pid1 = create({10, 1}), 
    Pid2 = create({10, 1}),
    add_peers(Pid0, [Pid1, Pid2]),
    add_peers(Pid1, [Pid0, Pid2]),
    add_peers(Pid2, [Pid0, Pid1]),

    start([Pid0, Pid1, Pid2]).

create({Value, Weight}) ->
    spawn_link(fun() -> loop(1, #node{value=Value, weight=Weight}) end).

add_peers(_, []) -> 
    ok;
add_peers(Pid, [H|T]) ->
    Pid ! {add, H},
    add_peers(Pid, T).

start(Pids) ->
    lists:map(fun(N) -> N ! tick end, Pids).
     
loop(Cycle, #node{value=Value, weight=Weight} = State) ->
    receive
        {push, From, {V0, W0}} ->
            V1 = Value/2,
            W1 = Weight/2,
            From ! {symmetric_push, self(), {V1, W1}},
            V2 = V0 + V1,
            W2 = W0 + W1, 
            loop(Cycle, State#node{weight=W2, value=V2});
        {symmetric_push, _From, {V0, W0}} ->
            V1 = V0 + Value,
            W1 = W0 + Weight,
            io:format("~p : v = ~p, w = ~p (~p) ~p~n", [self(), V1, W1, ((1/W1)*V1), Cycle]),
            loop(Cycle + 1, State#node{weight=W1, value=V1});
        {add, Node} ->
            loop(Cycle, State#node{peers=[Node | State#node.peers]});
        tick ->
            timer:send_after(1000, tick),

            {V, W} = lists:foldl(
                fun(Neighbour, {V0, W0}) ->
                    V1 = V0/2,
                    W1 = W0/2,
                    Neighbour ! {push, self(), {V1, W1}},
                    {V1, W1}
                end,
                {Value, Weight},
                State#node.peers
            ),
            loop(Cycle, State#node{value=V, weight=W});
        stop ->
            io:format("~p: w = ~p, v = ~p~n", [self(), Weight, Value]),
            State
    end.
