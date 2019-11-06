package com.taobao.yugong.common.db.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.yugong.common.utils.YuGongToStringStyle;

/**
 * 代表一个字段的信息
 *
 * @author agapple 2013-9-3 下午2:46:32
 * @since 3.0.0
 */
public class ColumnMeta {

    private String name;
    private int    type;

    public ColumnMeta(String columnName, int columnType){
        this.name = StringUtils.upperCase(columnName);// 统一为大写
        this.type = columnType;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public void setName(String name) {
        this.name = StringUtils.upperCase(name);
    }

    public void setType(int type) {
        this.type = type;
    }

    public ColumnMeta clone() {
        return new ColumnMeta(this.name, this.type);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, YuGongToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnMeta other = (ColumnMeta) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (type != other.type) return false;
        return true;
    }

}
