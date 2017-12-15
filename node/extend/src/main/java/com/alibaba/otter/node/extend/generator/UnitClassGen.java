package com.alibaba.otter.node.extend.generator;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by noah on 2017/12/13.
 * 生成按ID的小区处理器文件
 */
public class UnitClassGen {

    static String filePath = "//Users//noah//IdeaProjects//oeasy//otter//node//extend//src//main//java//com//alibaba//otter//node//extend//processor//UnitGens//";

    static String classPrefix = "UnitProcessor";

    static String template = "package com.alibaba.otter.node.extend.processor.UnitGens;\nimport com.alibaba.otter.node.extend.processor.UnitProcessor;\n/**\n* Created by noah on 2017/12/13.\n*/\npublic class %s%d extends UnitProcessor {\n}";


    public static void main(String[] args) throws IOException {
        for (int i = 1; i < 100; i++) {
            String content = String.format(template, classPrefix, i);
            File target = new File(filePath + classPrefix + i + ".java");
            target.createNewFile();
            Writer writer = new FileWriter(target);
            IOUtils.write(content, writer);
            IOUtils.closeQuietly(writer);
        }
    }

}
