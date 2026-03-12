library(plumber)

pr <- plumb("api/api.R")
pr$run(host = "127.0.0.1", port = 8000)
