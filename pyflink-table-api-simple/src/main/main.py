from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# create a table of messages
messages_table = table_env.from_elements(
    [
        (1, 'Hi', 'Colten'),
        (2, 'Hello', 'Alex'),
        (3, 'How are you?', 'Colten'),
        (4, 'Good, what is your name?', 'Alex'),
        (5, 'I am Colten, who are you?', 'Colten'),
        (6, 'Alex, nice to meet you', 'Alex')
    ],
    ['id', 'message', 'user']
)

# get colten's messages
colten_messages = (
    messages_table
    .where(col('user') == 'Colten')
    .select(col('message'), col('user'))
)

# print colten's messages
colten_messages.execute().print()
