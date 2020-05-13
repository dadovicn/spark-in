package com.wjl.count;

import com.wjl.count.keyword.WordCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CountApplication {

    @Autowired
    private WordCountService wordCountService;

    public static void main(String[] args) {
        // asdf asdf
        SpringApplication.run(CountApplication.class, args);

    }


}
