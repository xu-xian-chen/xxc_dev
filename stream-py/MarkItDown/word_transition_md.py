from markitdown import MarkItDown

# 创建一个 MarkItDown 实例

md = MarkItDown()
result = md.convert(r"E:\实训\专高六\cdh\Linux安装 MySQL.pdf")
markdown_content = result.text_content
markdown_text = result.markdown
print(markdown_content)