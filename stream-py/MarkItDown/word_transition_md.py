from markitdown import MarkItDown

# 创建一个 MarkItDown 实例

md = MarkItDown()
result = md.convert(r"C:\Users\55316\Desktop\seatunnel 配置文件大全.pdf")
markdown_content = result.text_content
markdown_text = result.markdown
print(markdown_content)