

question = open('Question.txt','r')

questions = [line.strip() for line in question.readlines()]


question.close()


ans = open('answer.txt','w')
user_score = 0
for q in questions:
    user_input = input(f"What is the Ans for {q}=")
    ans.write(f"{q}={user_input}\n")
    if user_input == '8':
        user_score += 1