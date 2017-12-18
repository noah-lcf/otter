import com.alibaba.fastjson.JSON;
import com.alibaba.otter.shared.common.model.config.data.TableRegex;
import com.alibaba.otter.shared.common.utils.RegexUtils;

import java.util.List;

/**
 * Created by noah on 2017/12/18.
 */
public class RegexTest {

    static String reg = "[{\"tableName\":\"t1\",\"columnRules\":{\"name\":\"a|b|c\"}},{\"tableName\":\"t2\",\"columnRules\":{\"name\":\"e|f|g\"}}]";


    public static void main(String[] args) {
        List<TableRegex> regexList = JSON.parseArray(reg, TableRegex.class);
        for (TableRegex regex : regexList) {
            System.out.println(regex);
            System.out.println(RegexUtils.findFirst("aadf", regex.getColumnRules().get("name")));
            System.out.println(RegexUtils.findFirst("eerq", regex.getColumnRules().get("name")));
        }


    }
}
