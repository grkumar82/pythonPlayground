"""
Given a string s and list of words, whenever there is a match between word from the
list and substring of s, insert characters into given string with '*1' at the start and '*12' at the end
"""
class Solution:

    def stringMatch(self, s, wrdList):
        """
        :param s:
        :return: str
        begin = *1
        end = *12
        """
        i, j = 0, 0
        ans = s
        for wrd in wrdList:
            i, j = 0, len(wrd)
            while j <= len(ans):
                if ans[i:j] == wrd:
                    # print(s[i:j], i, j)
                    ans = ans[:i] + '*1' + ans[i:j] + '*12' + ans[j:]
                    break
                else:
                    i += 1
                    j += 1
        return ans


s = Solution()
assert s.stringMatch('abc123xyz', ['abc', 'xyz']) == '*1abc*12123*1xyz*12'
