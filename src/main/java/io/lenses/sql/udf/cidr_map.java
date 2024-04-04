package io.lenses.sql.udf;

import io.lenses.sql.udf.UdfException;
import io.lenses.sql.udf.UserDefinedFunction2;
import io.lenses.sql.udf.datatype.DataType;
import io.lenses.sql.udf.value.StringValue;
import io.lenses.sql.udf.value.Value;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;

public class cidr_map implements UserDefinedFunction2 {

    @Override
    public DataType typeMapping(DataType dataType, DataType dataType1) throws UdfException {
        if (!dataType.isString() || !dataType1.isString()) {
            throw new UdfException("io.lenses.sql.cidr_map function expects two string arguments: a CIDR range and an IP address.");
        }
        return DataType.ltBoolean();

    }

    @Override
    public Value evaluate(Value value, Value value1) throws UdfException {
        if (!(value instanceof StringValue) || !(value1 instanceof StringValue)) {
            throw new UdfException("Invalid arguments. Expecting two strings: a CIDR range and an IP address.");
        }

        String cidr = ((StringValue) value).get();
        String ip = ((StringValue) value1).get();

        boolean result = isIpInRange(cidr, ip);
        return new StringValue(String.valueOf(result));
    }

    private boolean isIpInRange(String cidr, String ip) {
        try {
            SubnetUtils utils = new SubnetUtils(cidr);
            SubnetInfo info = utils.getInfo();
            return info.isInRange(ip);
        } catch (IllegalArgumentException e) {
            // Invalid CIDR/IP format
            return false;
        }
    }

    @Override
    public String name() {
        return "cidr_map";
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String owner() {
        return "AdamCNoonan";
    }

    // Add any additional helper methods if necessary
}
