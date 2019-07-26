package com.mxl.sparkmall.api;

import com.mxl.sparkmall.api.bean.Item;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.elasticsearch.jest.JestProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestEs {
    //@Autowired
    //ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    JestClient jestClient;

    @Test
    public void test01() throws Exception{
        //elasticsearchTemplate.createIndex(Item.class);
        //JestProperties
        Item item = new Item();
        item.setId(1L);
        item.setBrand("书籍");
        item.setCategory("科技");
        item.setImages("http://xx.img");
        item.setPrice(12.90);
        item.setTitle("未来简史");
        Index index = new Index.Builder(item).index("item_index").type("item").build();

        jestClient.execute(index);
    }
}
