/*******************************************************************************
 * metaXplorDB - Copyright (C) 2020 <CIRAD>
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.metaxplor.jobs.base;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import fr.cirad.tools.ProgressIndicator;

public interface IOpalServiceInvoker {
    public String makeBlastDb(String module, int projId, File fastaFile) throws Exception;
	public String getMakeBlastDbStatus(String jobId) throws Exception;
	public void cleanupProjectFiles(String module, int projId) throws IOException;
    public void cleanupDbFiles(String module) throws IOException;
	public Collection<String> blast(String sModule, String banks, String program, String sequence, String expect, String align, ProgressIndicator progress) throws IOException;
	public Collection<String> diamond(String sModule, String banks, String program, String sequence, String expect, String align, ProgressIndicator progress) throws IOException;
	public String phyloAssign(String module, String pplacerQueryHash, String mafftOption, ProgressIndicator progress) throws Exception;
	public String inspectRefPackage(String refPkgName) throws IOException;
}
