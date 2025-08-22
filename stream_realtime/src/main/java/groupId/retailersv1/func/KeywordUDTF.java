package groupId.retailersv1.func;


import groupId.retailersv1.stream.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))

/**
 * Title: KeywordUDTF
 * Author: hyx
 * Package: groupId.retailersv1.func
 * Date: 2025/8/21 22:00
 * Description:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)) {
            collect(Row.of(keyword));
        }
    }
}
