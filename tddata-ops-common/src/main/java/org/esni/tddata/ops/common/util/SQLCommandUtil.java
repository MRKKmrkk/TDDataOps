package org.esni.tddata.ops.common.util;

import org.esni.tddata.ops.common.exception.SQLCheckError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLCommandUtil {

    public enum Role {

        CREATE("create"),
        SELECT("select"),
        USE("use"),
        ALTER("alter"),
        DROP("drop"),
        DELETE("delete"),
        GRANT("grant"),
        INSERT("insert");

        private static final ArrayList<Role> ROLES = new ArrayList<Role>();

        static {
            ROLES.add(Role.CREATE);
            ROLES.add(Role.SELECT);
            ROLES.add(Role.USE);
            ROLES.add(Role.ALTER);
            ROLES.add(Role.DROP);
            ROLES.add(Role.DELETE);
            ROLES.add(Role.GRANT);
            ROLES.add(Role.INSERT);
        }

        private final String role;

        Role(String role) {

            this.role = role;

        }

        public String getRole() {

            return role;

        }

    }

    /**
     * 检测sql语句是否安全
     * @param sql 被检查的sql语句
     * @param workspace 该sql语句被执行的工作空间
     * @param notAllowRoles 不允许使用的权限
     * @return 是否安全
     */
    public static boolean isSafeSQLCommand(String sql, String workspace, Role...notAllowRoles) {

        if (sql.trim().isEmpty()) {
            throw new SQLCheckError("argument 'sql' can not be empty on method isSafeSQLCommand");
        }

        if (workspace.trim().isEmpty()) {
            throw new SQLCheckError("argument 'workspace' can not be empty on method isSafeSQLCommand");
        }

        sql = sql.toLowerCase();
        workspace = workspace.toLowerCase();

        // 检查权限
        for (Role notAllowRole : notAllowRoles) {

            if (sql.contains(notAllowRole.getRole())) return false;

        }

        // 检查危险表名
        Pattern pattern = Pattern.compile("(\\w.?)\\.\\w.?");
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {

            if (!matcher.group(0).startsWith(workspace)) return false;

        }

        return true;

    }

    public static ArrayList<Role> getRolesExcepted(Role... roles) {

        if (roles.length == 0) {
            return Role.ROLES;
        }

        ArrayList<Role> expectRoles = new ArrayList<>();
        List<Role> exceptedRoles = Arrays.asList(roles);
        for (Role role : Role.ROLES) {
            if (!exceptedRoles.contains(role)) expectRoles.add(role);
        }

        return expectRoles;

    }

}
