package ikang.datax.plugin.writer.titandbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by liutao on 16/6/8.
 */
public enum TitanDBWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("TitanDBWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("TitanDBWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("TitanDBWriter-02", "您填写的参数值不合法.");

    private final String code;
    private final String description;

    private TitanDBWriterErrorCode(String code,String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
