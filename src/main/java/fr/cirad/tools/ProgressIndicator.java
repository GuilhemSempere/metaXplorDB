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
package fr.cirad.tools;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
/**
 * class to handle the progress of a query / action 
 * @author petel, sempere
 */
public class ProgressIndicator {

    private static final Logger LOG = Logger.getLogger(ProgressIndicator.class);

    private static final HashMap<String, ProgressIndicator> progressIndicators = new HashMap<>();
    
	/** The us number format. */
	static private NumberFormat usNumberFormat = NumberFormat.getNumberInstance(Locale.US);

    private final String m_processId;
    private int m_currentStepProgress = 0;
    private short m_currentStepNumber = 0;
    private final List<String> m_stepLabels = new ArrayList<>();
    private String m_error = null;
    private String m_notificationEmail = null;
    private boolean m_fAborted = false;
    private boolean m_fComplete = false;
    private boolean m_fSupportsPercentage = true;
	private String m_description = null;

    public ProgressIndicator(String sProcessId, String[] stepLabels) {
        m_processId = sProcessId;
        this.m_stepLabels.addAll(Arrays.asList(stepLabels));
    }

    public void setPercentageEnabled(boolean fEnabled) {
        m_fSupportsPercentage = fEnabled;
    }

    public void addStep(String sStepLabel) {
        m_stepLabels.add(sStepLabel);
    }

    public String getProcessId() {
        return m_processId;
    }

    public int getCurrentStepProgress() {
        return m_currentStepProgress;
    }

    public void setCurrentStepProgress(int currentStepProgress) {
        if (m_fSupportsPercentage && (currentStepProgress < 0 || currentStepProgress > 100)) {
            LOG.warn("Invalid value for currentStepProgress: " + currentStepProgress);
        }
        this.m_currentStepProgress = currentStepProgress;
    }

    public short getCurrentStepNumber() {
        return m_currentStepNumber;
    }

    public void moveToNextStep() {
        m_currentStepNumber++;
        if (m_currentStepNumber > m_stepLabels.size()) {
            LOG.warn("Moving to unexisting step: " + m_currentStepNumber);
        }
        m_currentStepProgress = 0;
    }

    public short getStepCount() {
        return (short) m_stepLabels.size();
    }

    public String getError() {
        return m_error;
    }

    public void setError(String error) {
        this.m_error = error;
        remove();
    }

    public String getNotificationEmail() {
        return m_notificationEmail;
    }

    public void setNotificationEmail(String notificationEmail) {
        this.m_notificationEmail = notificationEmail;
    }

//    public String getProgressDescription() {
//        if (stepLabels.size() <= currentStepNumber) {
//            return "Please wait...";
//        }
//
//        return stepLabels.get(currentStepNumber) + "... " + (currentStepProgress == 0 ? "" : (currentStepProgress + (fSupportsPercentage ? "%" : "")));
//    }

    public void abort() {
        m_fAborted = true;
        remove();
    }
    
	private void remove()
	{
		new Timer().schedule(new TimerTask() {
		    @Override
		    public void run() {
		    	progressIndicators.remove(m_processId);
		    	LOG.debug("removed " + (hashCode()  + ": " + getProgressDescription()) + " for process " + m_processId);
		    }
		}, 1500);
	}

    public boolean hasAborted() {
        return m_fAborted;
    }

    public void markAsComplete() {
        m_fComplete = true;
    }

    public boolean isComplete() {
        return m_fComplete;
    }

    public static ProgressIndicator get(String sProcessID) {
        ProgressIndicator progress = progressIndicators.get(sProcessID);
        if (progress != null && progress.isComplete()) {
            LOG.debug("removing ProgressIndicator for process " + sProcessID);
            progress.remove();	// we don't want to keep them forever
        }
        return progress;
    }

    public static void registerProgressIndicator(ProgressIndicator progress) {
        progressIndicators.put(progress.getProcessId(), progress);
    }
    
	/**
	 * Sets the progress description.
	 *
	 * @param description the new progress description
	 */
	public void setProgressDescription(String description) {
		m_description = description;
	}
	
	/**
	 * Gets the progress description.
	 *
	 * @return the progress description
	 */
	public String getProgressDescription() {
		if (m_stepLabels.size() <= m_currentStepNumber)
			return "Please wait...";
		
		if (m_description != null)
			return m_description;

		return m_stepLabels.get(m_currentStepNumber) + "... " + (m_currentStepProgress == 0 ? "" : ((m_fSupportsPercentage ? m_currentStepProgress : usNumberFormat.format(m_currentStepProgress)) + (m_fSupportsPercentage ? "%" : "")));
	}
}
