package com.e2mg.bigdata.hadoop.mapjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 20:37
 */
public class TableBean implements Writable {

    private String id;
    private String pid;
    private String pname;
    private String flag;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        pid = dataInput.readUTF();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    public TableBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "TableBean{" +
                "id='" + id + '\'' +
                ", pid='" + pid + '\'' +
                ", pname='" + pname + '\'' +
                ", flag='" + flag + '\'' +
                '}';
    }
}
