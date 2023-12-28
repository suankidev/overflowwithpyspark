class User:
    def __init__(self, name, engagement):
        self.name = name
        self.engagement_metrics = engagement
        self.score = 0

    def __repr__(self):
        return f"<User {self.name} >"


def get_user_score(user):
    try:
        user.score = perform_calculation(user.engagement_metrics)
    except KeyError:
        print("Incorrect values provide to our calculation function.")
        raise  # reraise error which is raised in except block
    else:
        if user.score > 500:
            send_engagement_notification(user)


def perform_calculation(metrics):
    return metrics['click'] * 5 + metrics['hits'] * 2


def send_engagement_notification(user):
    print(f"Notification sent to {user}")


my_user = User(name='Sujeet', engagement={'click': 61, 'hits': 100})
get_user_score(my_user)

