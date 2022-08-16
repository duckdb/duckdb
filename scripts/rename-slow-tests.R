library(tidyverse)

here <- rprojroot::is_git_root$find_file

# build/debug/test/unittest -d yes 2>&1 > timings.txt
timings <- readLines(here("timings.txt"))

timings
timings_df <- rematch2::re_match(timings, "^.*(?<time>[0-9][.][0-9][0-9][0-9]) s: (?<desc>.*)$")

cum_timings_df <-
  timings_df %>%
  filter(!is.na(time)) %>%
  mutate(time = as.numeric(time)) %>%
  count(desc, wt = time, name = "time") %>%
  arrange(time) %>%
  mutate(cum_time = cumsum(time), id = row_number())

cum_timings_df %>%
  ggplot(aes(x = time, y = cum_time, color = id)) +
  geom_line() +
  scale_x_log10()

cum_timings_df %>%
  ggplot(aes(x = id, y = cum_time, color = time)) +
  geom_line() +
  scale_colour_continuous(trans = "log10")

cum_timings_cut <-
  cum_timings_df %>%
  filter(cum_time >= 200, str_detect(desc, "[.]test$"))

slow <- cum_timings_cut$desc
slow_renamed <- paste0(slow, "_coverage")

slow_renamed[fs::file_exists(here(slow_renamed))]
stopifnot(!any(fs::file_exists(here(slow_renamed))))

withr::with_dir(
  here(),
  fs::file_move(slow, slow_renamed)
)

walk2(slow_renamed, slow, ~ {
  text <- brio::read_lines(here(.x))
  text <- str_replace_all(text, fixed(.y), .x)
  brio::write_lines(text, here(.x))
})
