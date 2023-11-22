package com.alibaba.lindorm.contest.extendstructs.wal;


import com.alibaba.lindorm.contest.extendstructs.DBConstant;

/**
 * key，value组合。
 *
 * @author 陈翔
 */
public class WalEntry implements DBConstant {

  private int key;

  private ValEntity value;

  public WalEntry(int key, ValEntity value) {
    this.key = key;
    this.value = value;
  }

  public void toBytes(byte[] b){
    //int short小端存储
    b[0] = (byte) (key & 0xff);
    b[1] = (byte) (key >> 8 & 0xff);
    b[2] = (byte) (key >> 16 & 0xff);
    b[3] = (byte) (key >> 24 & 0xff);


    b[4] = (byte) (value.getCompressIndex() & 0xff);
    b[5] = (byte) (value.getCompressIndex() >> 8 & 0xff);

    b[6] = (byte) (value.getInternalIndex() & 0xff);
    b[7] = (byte) (value.getInternalIndex()  >> 8 & 0xff);
  }

  public Integer getKey() {
    return key;
  }

  public ValEntity getValue() {
    return value;
  }

  public ValEntity setValue(ValEntity value) {
    return this.value = value;
  }

  public void setKey(Integer key) {
    this.key = key;
  }

  @Override
  public String toString() {
    return "WalEntry{" +
            "key=" + key +
            ", value=" + value +
            '}';
  }
}
