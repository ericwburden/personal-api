logfile <- "~/personal-data/logs/heartbeat.log"

cat(
  paste(Sys.time(), "- job ran\n"),
  file = path.expand(logfile),
  append = TRUE
)
