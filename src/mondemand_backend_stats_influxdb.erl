-module (mondemand_backend_stats_influxdb).

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).
-behaviour (mondemand_backend_worker).

%% mondemand_backend_worker callbacks
-export ([ create/1,
           connected/1,
           connect/1,
           send/2,
           destroy/1 ]).

%% mondemand_server_backend callbacks
-export ([ start_link/1,
           process/1,
           required_apps/0,
           type/0
         ]).

%% mondemand_backend_stats_handler callbacks
-export ([ header/0,
           format_stat/10,
           separator/0,
           footer/0,
           handle_response/2
         ]).

%% supervisor callbacks
-export ([ init/1 ]).

-record (state, { url,
                  timeout }).

-compile({parse_transform, ct_expand}).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ( { local, ?MODULE }, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process
    (mondemand_backend_stats_influxdb_worker_pool, Event).

required_apps () ->
  [ crypto, public_key, ssl, lhttpc, lwes, mondemand ].

type () ->
  supervisor.

%%====================================================================
%% supervisor callbacks
%%====================================================================
init ([Config]) ->
  Number = proplists:get_value (number, Config, 16), % FIXME: replace default
  { ok,
    {
      {one_for_one, 10, 10},
      [
        { mondemand_backend_stats_influxdb_worker_pool,
          { mondemand_backend_worker_pool_sup, start_link,
            [ mondemand_backend_stats_influxdb_worker_pool,
              mondemand_backend_worker,
              Number,
              ?MODULE ]
          },
          permanent,
          2000,
          supervisor,
          [ ]
        }
      ]
    }
  }.

%%====================================================================
%% mondemand_backend_stats_handler callbacks
%%====================================================================
header () -> "".

separator () -> "\n".

format_stat (_Num, _Total, Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->
  _ActualPrefix = case Prefix of undefined -> ""; _ -> [ Prefix, "." ] end,
  ContextStr =
    case Context of
      [] -> "";
      L when is_list (L) ->
        [",",mondemand_server_util:join ([[K,"=",V] || {K, V} <- L ],",")]
    end,
  Line =
    [ normalize_metric_name(MetricName),
      ",program_id=",normalize_program_id(ProgId),
      ",host=",Host,
      ",type=",atom_to_list (MetricType),
      ContextStr,
      " value=",io_lib:fwrite ("~b ~b",[MetricValue, Timestamp])
    ],
  { ok, Line, 1, 0 }.

footer () -> "".

handle_response (Response, _Previous) ->
  error_logger:info_msg ("~p : got unexpected response ~p",[?MODULE, Response]),
  { 0, undefined }.

%%====================================================================
%% mondemand_backend_worker callbacks
%%====================================================================
create (Config) ->
  Url = proplists:get_value (url, Config),
  Timeout = proplists:get_value (timeout, Config),
  {ok, #state { url = Url, timeout = Timeout }}.

connected (#state { url = undefined }) -> false;
connected (_) -> true.

connect (State) -> {ok, State}.

send (State = #state {url = Url, timeout = Timeout}, Data) ->
  case catch lhttpc:request
         (Url, "POST",[], Data, Timeout, [{max_connections, 256}])
  of
    {ok, {{204,_},_,_}} ->
      { ok, State };
    E ->
      error_logger:error_msg ("influx responded with ~p",[E]),
      { error, State }
  end.

destroy (_) ->
  ok.

normalize_program_id (ProgId) ->
  % limit the character set for program_id's to alphanumeric, '_' and '.'
  {ok, RE} = ct_expand:term (re:compile ("[^a-zA-Z0-9_\\.]")),
  re:replace (ProgId, RE, <<"_">>, [global,{return,binary}]).

normalize_metric_name (MetricName) ->
  % limit the character set for metric names to alphanumeric and '_'
  {ok, RE} = ct_expand:term (re:compile ("[^a-zA-Z0-9_]")),
  % any non-alphanumeric or '_' characters are replaced by '_'
  re:replace (MetricName, RE, <<"_">>,[global,{return,binary}]).
