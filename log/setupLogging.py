import os
import json
import logging
import logging.config


def emit(self, record):
    """
    Overwrite the logging.handlers.SMTPHandler.emit function with SMTP_SSL.
    Emit a record.
    Format the record and send it to the specified addressees.
    """
    try:
        import smtplib
        from email.utils import formatdate

        port = self.mailport
        if not port:
            port = smtplib.SMTP_PORT
        smtp = smtplib.SMTP_SSL(self.mailhost, port, timeout=self._timeout)
        msg = self.format(record)
        msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
            self.fromaddr,
            ", ".join(self.toaddrs),
            self.getSubject(record),
            formatdate(),
            msg,
        )
        if self.username:
            smtp.ehlo()
            smtp.login(self.username, self.password)
        smtp.sendmail(self.fromaddr, self.toaddrs, msg)
        smtp.quit()
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        self.handleError(record)


def setupLogging(
    default_path="/home/ubuntu/crawlers/scripts/viagens/ryanair/log/logging.conf",
    default_level=logging.INFO,
    env_key="LOG_CFG",
    error_logfile=None,
    info_logfile=None,
    logger="",
):
    """
    Setup logging configuration.
    Extracted from:
    https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)

        # Overwrite error_logfile/info_logfile
        if error_logfile is not None:
            config["handlers"]["error_file_handler"]["filename"] = error_logfile
        if info_logfile is not None:
            config["handlers"]["info_file_handler"]["filename"] = info_logfile

        logging.config.dictConfig(config)
        if logger:
            logger = logging.getLogger(logger)
            logger.info("Loading logger config from %s" % path)
    else:
        logging.basicConfig(level=default_level)
        if logger:
            logger = logging.getLogger(logger)
            logger.info("Loading default logger config")

    # Patch for accessing SMPT_SSL
    logging.handlers.SMTPHandler.emit = emit
