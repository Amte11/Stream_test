package groupId.retailersv1.func;

/**
 * Title: DimFunction
 * Author: hyx
 * Package: groupId.retailersv1.func
 * Date: 2025/8/23 11:03
 * Description:
 */
import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}