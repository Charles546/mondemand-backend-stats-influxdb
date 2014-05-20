-module (mondemand_backend_stats_influxdb).

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).
-behaviour (mondemand_backend_worker).

%% mondemand_backend_worker callbacks
-export ([ create/1,
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
           format_stat/8,
           separator/0,
           footer/0,
           handle_response/2
         ]).

%% supervisor callbacks
-export ([ init/1 ]).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ( { local, ?MODULE }, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process
    (mondemand_backend_stats_influxdb_worker_pool, Event).

required_apps () ->
  [ crypto, public_key, ssl, lhttpc ].

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
header () -> "[".

separator () -> ",\n".

format_stat (Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->
  _ActualPrefix = case Prefix of undefined -> ""; _ -> [ Prefix, "." ] end,
  {ContextKeys, ContextValues} =
    case Context of
      [] -> {"",""};
      L when is_list (L) ->
        lists:foldl (
          fun ({K, V}, {CK, CV}) -> {[CK,",\"",K,"\""], [CV,",\"",V,"\""]} end,
          { "", "" },
          Context
        )
    end,
  [ "{\"name\":\"",MetricName,"\",",
    "\"columns\":[\"time\",\"value\",\"type\",\"program_id\",\"host\"", ContextKeys,"],",
    "\"points\":[[",io_lib:fwrite ("~b,~b",[Timestamp,MetricValue]),",",
                  "\"",MetricType,"\",",
                  "\"",ProgId,"\",",
                  "\"",Host,"\"",
                  ContextValues,
               "]]"
    "}" ].

footer () -> "]\n".

handle_response (Response, _Previous) ->
  error_logger:info_msg ("~p : got unexpected response ~p",[?MODULE, Response]),
  { 0, undefined }.

%%====================================================================
%% mondemand_backend_worker callbacks
%%====================================================================
create (Config) ->
  Url = proplists:get_value (url, Config),
  Timeout = proplists:get_value (timeout, Config),
  { Url, Timeout }.

send ({Url, Timeout}, Data) ->
  case catch lhttpc:request
         (Url, "POST",[], Data, Timeout, [{max_connections, 256}])
  of
    {ok, {{200,_},_,_}} ->
      ok;
    E -> error_logger:error_msg ("influx responded with ~p",[E]),
      error
  end.

destroy (_) ->
  ok.
