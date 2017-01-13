##说明

####修改RegexHbaseEventSerializer，添加以下功能：
 + 1、rowKey前缀：
```
设置：setPrefix：true
               rowKeyPrefix ：要设置前缀的字段
                rowKeyPrefixFrom：字段来自哪里
                    * body ： 来自event body中，rowKeyPrefix 填写body中对应的字段
                    * head ：来自event head 中，rowKeyPrefix 填写head 中对应的字段
                    * self：自定义，rowKeyPrefix 为自己设置的值
```
 + 2、rowKey后缀：
```
设置：setPrefix：true
               rowKeySuffix：要设置后缀的字段
                rowKeySuffixFrom：字段来自哪里
                    * body ： 来自event body中，rowKeyPrefix 填写body中对应的字段
                    * head ：来自event head 中，rowKeyPrefix 填写head 中对应的字段
                    * self：自定义，rowKeyPrefix 为自己设置的值
```
```
+ 3、是否将指定的rowKey设置为timepstamp的形式：
        rowKeyToTimeStamp：true
        rowKeyFormat：要转换为时间戳的日期格式。
```

  