# Describe without scientific notation
df.describe().apply(lambda s: s.apply(lambda x: format(x, 'g')))