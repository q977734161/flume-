##˵��

####�޸�RegexHbaseEventSerializer��������¹��ܣ�
 + 1��rowKeyǰ׺��
```
���ã�setPrefix��true
               rowKeyPrefix ��Ҫ����ǰ׺���ֶ�
                rowKeyPrefixFrom���ֶ���������
                    * body �� ����event body�У�rowKeyPrefix ��дbody�ж�Ӧ���ֶ�
                    * head ������event head �У�rowKeyPrefix ��дhead �ж�Ӧ���ֶ�
                    * self���Զ��壬rowKeyPrefix Ϊ�Լ����õ�ֵ
```
 + 2��rowKey��׺��
```
���ã�setPrefix��true
               rowKeySuffix��Ҫ���ú�׺���ֶ�
                rowKeySuffixFrom���ֶ���������
                    * body �� ����event body�У�rowKeyPrefix ��дbody�ж�Ӧ���ֶ�
                    * head ������event head �У�rowKeyPrefix ��дhead �ж�Ӧ���ֶ�
                    * self���Զ��壬rowKeyPrefix Ϊ�Լ����õ�ֵ
```
```
+ 3���Ƿ�ָ����rowKey����Ϊtimepstamp����ʽ��
        rowKeyToTimeStamp��true
        rowKeyFormat��Ҫת��Ϊʱ��������ڸ�ʽ��
```

  