import src
import datetime
import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats
from alive_progress import alive_bar


dfs = src.load_tables("Submission")
dfq = src.load_tables("Question")
dfc = src.load_tables("Course")


dfs = dfs.filter(
    pl.col("timeStarted") > datetime.date.today(), pl.col("userSolution").list.len() > 0
)
print(dfs["correct"].mean() * 100)
endd
dt = dfs.filter(pl.col("timeSpent") > 0)["timeSpent"].to_numpy() / 1000 / 60  # minutes

print(np.median(dt) / 60)  # avg minutes per question

# lam = 1/np.median(dt)
range_ = (0.1, np.percentile(dt, 95.0))
print(range_)
x = np.linspace(*range_, 1000)

model_names = ["chi2", "expon", "fatiguelife", "fisk", "gamma", "pareto"]
model_names = ["gamma"]
models = [getattr(scipy.stats, m) for m in model_names]

with alive_bar(len(model_names)) as bar:
    for name, model in zip(model_names, models):
        bar.title = name
        rv = model(*model.fit(dt[dt < range_[1]], method="MLE"))
        bar()
        rvx = rv.pdf(x)
        plt.plot(x, rvx, label=name)

plt.hist(dt, bins=100, range=range_, density=True, alpha=0.5, color="k")
plt.yscale("log")
plt.xlabel("Minutes spent")
plt.ylabel("Probability density")
plt.legend()
plt.grid()
plt.show()
