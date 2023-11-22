package com.alibaba.lindorm.contest.extendstructs.wal;

import com.alibaba.lindorm.contest.MetaIterator;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.util.Closeables;
import com.alibaba.lindorm.contest.util.MMapUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.SIZE_OF_WAL_RECORD;


/**
 * @author 陈翔
 */
public class MmapWalMetaIterator implements MetaIterator<byte[]> {
  public static Logger logger = TSDBLog.getLogger();

  private FileChannel pipe;

  private MappedByteBuffer mappedByteBuffer = null;

  private int offset = 0;

  private long fileSize;

  private boolean end;

  private boolean checkEnd;

  public MmapWalMetaIterator(File file) throws FileNotFoundException {
    this(file, true);
  }

  public MmapWalMetaIterator(File file, boolean checkEnd) throws FileNotFoundException {
    this.checkEnd = checkEnd;
    pipe = new FileInputStream(file).getChannel();
    end = false;
    try {
      fileSize = pipe.size();
      mappedByteBuffer = pipe.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
      mappedByteBuffer.order(ByteOrder.BIG_ENDIAN);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(pipe);
  }

  @Override
  public boolean hasNext() {
    return !end && offset < fileSize;
  }

  @Override
  public byte[] next() {
    if (checkEnd && mappedByteBuffer.remaining() < SIZE_OF_WAL_RECORD) {
      end = true;
      return null;
    }
    byte[] keyAndVlogSeq = new byte[SIZE_OF_WAL_RECORD];
    mappedByteBuffer.get(keyAndVlogSeq, 0, SIZE_OF_WAL_RECORD);
    offset += SIZE_OF_WAL_RECORD;
    return keyAndVlogSeq;
  }

  @Override
  public long getFileSize() {
    return fileSize;
  }

  @Override
  public int getRecordSize() {
    return (int) (fileSize / SIZE_OF_WAL_RECORD);
  }

  @Override
  public int getActualRecordSize() {
    return offset / SIZE_OF_WAL_RECORD;
  }

  @Override
  public void freeUp() {
    if (mappedByteBuffer != null) {
      MMapUtil.unmap(mappedByteBuffer);
      mappedByteBuffer = null;
    }
  }
}
