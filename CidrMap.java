package io.lenses.sql.udf;

import io.lenses.sql.udf.datatype.DataType;
import io.lenses.sql.udf.value.StringValue;
import io.lenses.sql.udf.value.Value;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;

public class CidrMap implements UserDefinedFunction2 {

    @Override
    public DataType typeMapping(DataType... types) throws UdfException {
        if (types.length != 2 || !types[0].isString() || !types[1].isString()) {
            throw new UdfException("CidrMap function expects two string arguments: a CIDR range and an IP address.");
        }
        return DataType.BOOLEAN;
    }

    @Override
    public Value evaluate(Value... args) throws UdfException {
        if (args.length != 2 || !(args[0] instanceof StringValue) || !(args[1] instanceof StringValue)) {
            throw new UdfException("Invalid arguments. Expecting two strings: a CIDR range and an IP address.");
        }

        String cidr = ((StringValue) args[0]).get();
        String ip = ((StringValue) args[1]).get();

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
        return "cidrmap";
    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public String owner() {
        return "AdamCNoonan";
    }

    // Add any additional helper methods if necessary
}
