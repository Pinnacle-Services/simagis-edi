-- ADD SESSION STATUS:
ALTER TABLE XReg.dbo.SESSION ADD STATUS VARCHAR(16) DEFAULT '';
ALTER TABLE XReg.dbo.SESSION ADD FILES_IN INT;
ALTER TABLE XReg.dbo.SESSION ADD FILES_DONE INT;
ALTER TABLE XReg.dbo.SESSION ADD OPTIMIZATIONS_IN INT;
ALTER TABLE XReg.dbo.SESSION ADD OPTIMIZATIONS_DONE INT;
ALTER TABLE XReg.dbo.SESSION ADD STATUS_TIME DATETIME;
