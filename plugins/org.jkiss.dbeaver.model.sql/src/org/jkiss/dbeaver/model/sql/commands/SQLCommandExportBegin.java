/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.model.sql.commands;

import com.google.gson.Gson;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.data.json.JSONUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.*;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Control command handler for @export_begin.
 * Starts a persistent export block: all subsequent queries will be exported
 * until @export_end is encountered.
 */
public class SQLCommandExportBegin implements SQLControlCommandHandler {

    public static final String BEGIN_MODE_KEY = "_beginMode";

    @NotNull
    @Override
    public SQLControlResult handleCommand(@NotNull DBRProgressMonitor monitor, @NotNull SQLControlCommand command, @NotNull SQLScriptContext scriptContext) throws DBException {
        final Map<String, Object> params;

        try {
            params = new HashMap<>(JSONUtils.parseMap(new Gson(), new StringReader(command.getParameter())));
        } catch (Exception e) {
            throw new DBException("Invalid syntax. Use '@export_begin {\"type\": <type>, \"producer\": {...}, \"consumer\": {...}, \"processor\": {...}}'", e);
        }

        params.put(BEGIN_MODE_KEY, Boolean.TRUE);
        scriptContext.setPragma(SQLPragmaHandler.PRAGMA_EXPORT, params);

        return SQLControlResult.success();
    }
}
