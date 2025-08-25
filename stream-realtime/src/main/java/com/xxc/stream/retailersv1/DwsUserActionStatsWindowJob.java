package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.EnvironmentSettingUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class DwsUserActionStatsWindowJob {

    // ---- helpers ----
    private static long tsOf(JSONObject j){
        long ts = j.getLongValue("ts");
        if (ts == 0L) {
            ts = j.getLongValue("event_time");
        }
        return ts == 0L ? System.currentTimeMillis() : ts;
    }

    private static String userIdOf(JSONObject j){
        JSONObject common = j.getJSONObject("common");
        if (common != null) {
            String uid = common.getString("uid");
            if (uid != null && !uid.isEmpty() && !"null".equalsIgnoreCase(uid)) {
                return uid;
            }
            String mid = common.getString("mid");
            if (mid != null && !mid.isEmpty()) {
                return mid;
            }
        }
        // 兜底
        String uid = j.getString("uid");
        if (uid != null && !uid.isEmpty()) {
            return uid;
        }
        return j.getString("mid");
    }

    // 侧输出：脏 display / action
    private static final OutputTag<String> BAD_DISPLAY = new OutputTag<String>("bad-display"){ };
    private static final OutputTag<String> BAD_ACTION  = new OutputTag<String>("bad-action"){ };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // ====== 配置 ======
        final String BOOTSTRAP  = ConfigUtils.getString("kafka.bootstrap.servers");
        final String SRC_DISP   = ConfigUtils.getString("kafka.display.log");
        final String SRC_ACT    = ConfigUtils.getString("kafka.action.log");
        final String OUT_TOPIC  = ConfigUtils.getString("kafka.dws.user.action");
        final int WIN_MIN       = ConfigUtils.getInt("dws.window.minutes");

        final boolean DEBUG     = true; // 需要可改为配置

        final WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, ts) -> {
                    try { return tsOf(JSON.parseObject(s)); } catch (Exception e) { return System.currentTimeMillis(); }
                })
                .withIdleness(Duration.ofSeconds(10));

        // ====== Kafka Sources：随机 group 让 earliest 生效 ======
        String groupId = "dws-user-action-" + UUID.randomUUID();
        KafkaSource<String> dispSrc = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_DISP, groupId, OffsetsInitializer.earliest());
        KafkaSource<String> actSrc  = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_ACT,  groupId, OffsetsInitializer.earliest());

        // ====== DISPLAY：解析 & 兼容嵌套 ======
        SingleOutputStreamOperator<JSONObject> display = env.fromSource(dispSrc, wm, "dwd-log-display")
                .process(new ProcessFunction<String, JSONObject>() {
                    int badCnt = 0;
                    @Override
                    public void processElement(String s, Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject j = JSON.parseObject(s);

                            // ① 若是嵌套在 display 里（你的样例），把 sku 信息提到顶层
                            JSONObject disp = j.getJSONObject("display");
                            if (disp != null) {
                                String item = disp.getString("item");
                                String itemType = disp.getString("item_type");
                                if (item != null && !item.isEmpty()) {
                                    j.put("item", item);
                                }
                                if (itemType != null && !itemType.isEmpty()) {
                                    j.put("item_type", itemType);
                                }
                            }

                            // ② 兼容已展开或仅有 sku_id 的情况
                            if ((j.getString("item") == null || j.getString("item").isEmpty())
                                    && j.getString("sku_id") != null) {
                                j.put("item", j.getString("sku_id"));
                                j.put("item_type", "sku_id");
                            }

                            String item = j.getString("item");
                            String itemType = j.getString("item_type");
                            if ("sku_id".equalsIgnoreCase(itemType) && item != null && !item.isEmpty()) {
                                out.collect(j);
                            } else {
                                if (badCnt++ < 10) {
                                    ctx.output(BAD_DISPLAY, "filter(display): " + s);
                                }
                            }
                        } catch (Exception e) {
                            ctx.output(BAD_DISPLAY, "parseErr(display): " + s);
                        }
                    }
                });

        SingleOutputStreamOperator<JSONObject> action = env.fromSource(actSrc, wm, "dwd-log-action")
                .process(new ProcessFunction<String, JSONObject>() {
                    int badCnt = 0;
                    @Override
                    public void processElement(String s, Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject root = JSON.parseObject(s);

                            // 情形 A：数组 actions[]
                            JSONArray arr = root.getJSONArray("actions");
                            if (arr != null && !arr.isEmpty()) {
                                for (int i = 0; i < arr.size(); i++) {
                                    JSONObject a = arr.getJSONObject(i);
                                    // 把公共字段 & 时间带过来
                                    JSONObject j = new JSONObject(true);
                                    j.putAll(root);
                                    j.putAll(a); // action_id / item / item_type / ts(若有)

                                    // 缺省时把根 ts 赋给行动
                                    if (j.getLongValue("ts") == 0L && a.getLongValue("ts") == 0L) {
                                        j.put("ts", root.getLongValue("ts"));
                                    }

                                    if (normalizeAction(j)) {
                                        out.collect(j);
                                    } else {
                                        if (badCnt++ < 10) {
                                            ctx.output(BAD_ACTION, "filter(action[arr]): " + a.toJSONString());
                                        }
                                    }
                                }
                                return;
                            }

                            // 情形 B：嵌套 action 对象
                            JSONObject act = root.getJSONObject("action");
                            if (act != null) {
                                JSONObject j = new JSONObject(true);
                                j.putAll(root);
                                j.putAll(act);
                                if (j.getLongValue("ts") == 0L && act.getLongValue("ts") == 0L) {
                                    j.put("ts", root.getLongValue("ts"));
                                }
                                if (normalizeAction(j)) {
                                    out.collect(j);
                                } else if (badCnt++ < 10) {
                                    ctx.output(BAD_ACTION, "filter(action[obj]): " + act.toJSONString());
                                }
                                return;
                            }

                            // 情形 C：顶层 action 字段
                            if (normalizeAction(root)) {
                                out.collect(root);
                            } else {
                                if (badCnt++ < 10) {
                                    ctx.output(BAD_ACTION, "filter(action[top]): " + s);
                                }
                            }
                        } catch (Exception e) {
                            ctx.output(BAD_ACTION, "parseErr(action): " + s);
                        }
                    }

                    // 归一化 + 过滤口径：仅要 sku 级点击/收藏/加购
                    private boolean normalizeAction(JSONObject j) {
                        // 若没有 item，尝试从 sku_id 回填
                        if ((j.getString("item") == null || j.getString("item").isEmpty())
                                && j.getString("sku_id") != null) {
                            j.put("item", j.getString("sku_id"));
                            j.put("item_type", "sku_id");
                        }
                        String aid = j.getString("action_id");
                        String item = j.getString("item");
                        String itemType = j.getString("item_type");

                        boolean isInterested =
                                "click".equalsIgnoreCase(aid) ||
                                "good_click".equalsIgnoreCase(aid) ||
                                "good_detail_click".equalsIgnoreCase(aid) ||
                                "favor_add".equalsIgnoreCase(aid) ||
                                "cart_add".equalsIgnoreCase(aid);

                        return isInterested && "sku_id".equalsIgnoreCase(itemType) && item != null && !item.isEmpty();
                    }
                });

        // 调试：侧输出看看被过滤/脏数据（最多各 10 条）
        if (DEBUG) {
            display.getSideOutput(BAD_DISPLAY).print();
            action.getSideOutput(BAD_ACTION).print();
        }

        // ====== 聚合：DISPLAY（曝光） ======
        DataStream<JSONObject> dispAgg = display
                .keyBy(j -> j.getString("item")) // 此时一定非空
                .window(TumblingEventTimeWindows.of(Time.minutes(WIN_MIN)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String skuId, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long exposure = 0L;
                        Set<String> users = new HashSet<>();
                        for (JSONObject j : elements) {
                            exposure++;
                            users.add(userIdOf(j));
                        }
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("stt", ctx.window().getStart())
                         .fluentPut("edt", ctx.window().getEnd())
                         .fluentPut("sku_id", skuId)
                         .fluentPut("exposure_ct", exposure)
                         .fluentPut("exposure_user_ct", users.size());
                        out.collect(o);
                    }
                });

        // ====== 聚合：ACTION（点击/收藏/加购） ======
        DataStream<JSONObject> actAgg = action
                .keyBy(j -> j.getString("item")) // 一定非空
                .window(TumblingEventTimeWindows.of(Time.minutes(WIN_MIN)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String skuId, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long click=0, favor=0, cart=0;
                        Set<String> clickUsers = new HashSet<>();
                        Set<String> favorUsers = new HashSet<>();
                        Set<String> cartUsers  = new HashSet<>();

                        for (JSONObject j : elements) {
                            String aid = j.getString("action_id");
                            String uid = userIdOf(j);
                            if (aid == null) {
                                continue;
                            }
                            switch (aid) {
                                case "click":
                                case "good_click":
                                case "good_detail_click":
                                    click++; clickUsers.add(uid); break;
                                case "favor_add":
                                    favor++; favorUsers.add(uid); break;
                                case "cart_add":
                                    cart++; cartUsers.add(uid); break;
                                default:
                                    // ignore
                            }
                        }
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("stt", ctx.window().getStart())
                         .fluentPut("edt", ctx.window().getEnd())
                         .fluentPut("sku_id", skuId)
                         .fluentPut("click_ct", click)
                         .fluentPut("click_user_ct", clickUsers.size())
                         .fluentPut("favor_ct", favor)
                         .fluentPut("favor_user_ct", favorUsers.size())
                         .fluentPut("cart_ct", cart)
                         .fluentPut("cart_user_ct", cartUsers.size());
                        out.collect(o);
                    }
                });

        // ====== 合并两个聚合流（同窗口、同 sku） ======
        DataStream<JSONObject> merged = dispAgg.union(actAgg)
                .keyBy(j -> j.getLongValue("stt")+"|"+j.getLongValue("edt")+"|"+j.getString("sku_id"))
                .reduce((a, b) -> {
                    JSONObject r = new JSONObject(true);
                    String sku = a.getString("sku_id") != null ? a.getString("sku_id") : b.getString("sku_id");
                    long stt = a.getLongValue("stt") != 0 ? a.getLongValue("stt") : b.getLongValue("stt");
                    long edt = a.getLongValue("edt") != 0 ? a.getLongValue("edt") : b.getLongValue("edt");
                    r.fluentPut("stt", stt).fluentPut("edt", edt).fluentPut("sku_id", sku)
                     .fluentPut("exposure_ct", a.getLongValue("exposure_ct") + b.getLongValue("exposure_ct"))
                     .fluentPut("exposure_user_ct", a.getLongValue("exposure_user_ct") + b.getLongValue("exposure_user_ct"))
                     .fluentPut("click_ct", a.getLongValue("click_ct") + b.getLongValue("click_ct"))
                     .fluentPut("click_user_ct", a.getLongValue("click_user_ct") + b.getLongValue("click_user_ct"))
                     .fluentPut("favor_ct", a.getLongValue("favor_ct") + b.getLongValue("favor_ct"))
                     .fluentPut("favor_user_ct", a.getLongValue("favor_user_ct") + b.getLongValue("favor_user_ct"))
                     .fluentPut("cart_ct", a.getLongValue("cart_ct") + b.getLongValue("cart_ct"))
                     .fluentPut("cart_user_ct", a.getLongValue("cart_user_ct") + b.getLongValue("cart_user_ct"));
                    return r;
                });

        // ====== Sink 到 Kafka（你指定的写法）======
        merged.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) { return jsonObject.toJSONString(); }
        }).sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, OUT_TOPIC)).name("sink-dws-user-action-stats");

        if (DEBUG) {
            merged.map(j -> "[DWS-USER-ACTION] " + j.toJSONString()).print();
        }

        env.execute("DWS User Action Stats Window");
    }
}
