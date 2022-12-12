//package cn.com.servyou.fintech.botfactory.kbase.udf
//
//import cn.com.servyou.fintech.botfactory.kbase.domain.BinLogRowData
//import cn.com.servyou.fintech.botfactory.kbase.domain.EventType
//import cn.com.servyou.fintech.kumiho.utils.StrUtil
//import cn.hutool.core.io.FileUtil
//import cn.hutool.json.JSONUtil
//import org.apache.flink.api.common.typeinfo.Types.INT
//import org.apache.flink.api.common.typeinfo.Types.STRING
//import org.apache.flink.table.annotation.DataTypeHint
//import org.apache.flink.table.annotation.FunctionHint
//import org.apache.flink.table.functions.TableFunction
//import org.apache.flink.types.Row
//import org.apache.flink.types.RowKind
//import org.slf4j.LoggerFactory
//import java.io.File
//
//
///***
// * 阿里云ECS部署此flink程序
// * source: 税屋原始mysql的binlog
// * sink: 阿里云rocketmq
// */
//@FunctionHint(
//        input = [DataTypeHint("STRING")],
//        output = DataTypeHint("ROW<" +
//                "rowKind STRING," +
//                "title STRING," +
//                "content STRING," +
//                "classid STRING," +
//                "befrom STRING," +
//                "truetime INT," +
//                "lastdotime INT," +
//                "newstime INT," +
//                "writer STRING," +
//                "smalltext STRING," +
//                "id STRING," +
//                "filename STRING," +
//                "onclick INT," +
//                "firsttitle INT," +
//                "ftitle STRING" +
//                ">")
//)
//class Shui5BinLogUdtf : TableFunction<Row>() {
//    private val logger = LoggerFactory.getLogger(this::class.java)
//
//    /**
//     * udf调用实际执行函数
//     */
//    @Throws(Exception::class)
//    fun eval(txRows: String?) {
//        // transaction rows反序列化
//        val binlogRows: List<BinLogRowData> = JSONUtil.toList(txRows, BinLogRowData::class.java)
//        for (binlogRow in binlogRows) {
//            val diffColumn = mutableListOf<String>()
//            if (binlogRow.type == EventType.UPDATE){
//                binlogRow.after?.keys?.forEach {
//                    if (binlogRow.before!![it] != binlogRow.after!![it]){
//                        diffColumn.add(it)
//                    }
//                }
//            }
//            if (diffColumn.size == 1 && (diffColumn[0] == "onclick" || diffColumn[0] == "diggtop")){
//                logger.debug("id:${binlogRow.after!!["id"]}:${diffColumn[0]}变化不做更新")
//                continue
//            }
//            diffColumn.forEach {
//                logger.info(it + ":" + binlogRow.before!![it] + "->"+binlogRow.after!![it])
//            }
//            val fieldValues = extractRowData(binlogRow)
//            val row = Row.ofKind(binlogRow.toRowKind(), *fieldValues.toTypedArray())
//            // 输出多个结果,返回顺序需要和output的DataTypeHint一致
//            logger.info("collect,kind:{},id:{}", fieldValues[0], fieldValues[10])
//            collect(row)
//        }
//    }
//
//    fun extractRowData(binlogRow: BinLogRowData): MutableList<Any?> {
//        try {
//            // 根据binlog的RowKind获取对应的数据
//            val rowData = when (binlogRow.toRowKind()) {
//                RowKind.INSERT -> binlogRow.after
//                RowKind.DELETE -> binlogRow.before
//                RowKind.UPDATE_AFTER -> binlogRow.after
//                RowKind.UPDATE_BEFORE -> throw java.lang.Exception("unsupported")
//            }!!
//            val content= FileUtil.readString(File("/data/www.shui5.cn/d/txt/${rowData["newstext"]}.php"), Charsets.UTF_8)
//            rowData["content"] =formatContent(content)
//
//            // 字段按顺序输出
//            val defaultFieldMap = mapOf(
//                    "title" to STRING,
//                    "content" to STRING,
//                    "classid" to STRING,
//                    "befrom" to STRING,
//                    "truetime" to INT,
//                    "lastdotime" to INT,
//                    "newstime" to INT,
//                    "writer" to STRING,
//                    "smalltext" to STRING,
//                    "id" to STRING,
//                    "filename" to STRING,
//                    "onclick" to INT,
//                    "firsttitle" to INT,
//                    "ftitle" to STRING
//            )
//            // 字段类型转换
//            val fieldValues = mutableListOf<Any?>()
//            fieldValues.add(binlogRow.type!!.name)
//            defaultFieldMap.forEach {
//                if (it.value == STRING) {
//                    fieldValues.add(rowData[it.key].toString())
//                } else if (it.value == INT) {
//                    fieldValues.add(rowData[it.key].toString().toInt())
//                }
//            }
//            return fieldValues
//        } catch (e: Exception) {
//            throw RuntimeException("Shui5BinLogUdtf处理失败:${JSONUtil.toJsonStr(binlogRow)}", e)
//        }
//    }
//
//     fun formatContent(content: String?): String? {
//        return if (StrUtil.isNotBlank(content)) {
//            content!!.replace("\\\"", "\"").replace("<? exit();?>", "");
//        } else {
//            content
//        }
//    }
//}
//
