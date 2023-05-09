package com.lion.interceptors;

import com.lion.util.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: ETLInterceptor
 * @Author: Lion
 * @Date: 2023/5/1 20:09
 * @Description:
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String message = new String(event.getBody(), StandardCharsets.UTF_8);
        if(JSONUtil.isJSONValidate(message)){
            event.getHeaders().put("type","true");
        }else {
            event.getHeaders().put("type","false");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            Event next = iterator.next();
            intercept(next);
            String flag = next.getHeaders().get("type");
            if("false".equals(flag)){
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
