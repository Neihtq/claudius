import { Fragment } from 'react'
import type { ReactNode } from 'react'

type Block =
  | { type: 'heading'; level: number; text: string }
  | { type: 'divider' }
  | { type: 'paragraph'; lines: string[] }
  | { type: 'unordered-list'; items: string[] }
  | { type: 'ordered-list'; items: string[] }
  | { type: 'blockquote'; lines: string[] }
  | { type: 'table'; headers: string[]; aligns: Array<'left' | 'center' | 'right'>; rows: string[][] }
  | { type: 'code'; language: string; content: string }

function splitTableRow(line: string): string[] {
  const trimmed = line.trim().replace(/^\|/, '').replace(/\|$/, '')
  return trimmed.split('|').map((cell) => cell.trim())
}

function isTableDivider(line: string): boolean {
  const cells = splitTableRow(line)
  return (
    cells.length > 0 &&
    cells.every((cell) => /^:?-{3,}:?$/.test(cell))
  )
}

function parseTableAlignments(line: string): Array<'left' | 'center' | 'right'> {
  return splitTableRow(line).map((cell) => {
    const starts = cell.startsWith(':')
    const ends = cell.endsWith(':')
    if (starts && ends) return 'center'
    if (ends) return 'right'
    return 'left'
  })
}

function parseBlocks(markdown: string): Block[] {
  const lines = markdown.split('\r\n').join('\n').split('\n')
  const blocks: Block[] = []
  let i = 0

  while (i < lines.length) {
    const line = lines[i]
    const trimmed = line.trim()

    if (!trimmed) {
      i += 1
      continue
    }

    if (/^(\*\s*){3,}$/.test(trimmed) || /^(-\s*){3,}$/.test(trimmed) || /^(_\s*){3,}$/.test(trimmed)) {
      blocks.push({ type: 'divider' })
      i += 1
      continue
    }

    const fenceMatch = line.match(/^```([^\s`]*)\s*$/)
    if (fenceMatch) {
      const content: string[] = []
      i += 1
      while (i < lines.length && !lines[i].match(/^```\s*$/)) {
        content.push(lines[i])
        i += 1
      }
      if (i < lines.length) {
        i += 1
      }
      blocks.push({
        type: 'code',
        language: fenceMatch[1] ?? '',
        content: content.join('\n'),
      })
      continue
    }

    const headingMatch = line.match(/^(#{1,6})\s+(.+?)\s*$/)
    if (headingMatch) {
      blocks.push({
        type: 'heading',
        level: headingMatch[1].length,
        text: headingMatch[2],
      })
      i += 1
      continue
    }

    if (/^>\s?/.test(line)) {
      const quoteLines: string[] = []
      while (i < lines.length && /^>\s?/.test(lines[i])) {
        quoteLines.push(lines[i].replace(/^>\s?/, ''))
        i += 1
      }
      blocks.push({ type: 'blockquote', lines: quoteLines })
      continue
    }

    if (/^[-*+]\s+/.test(line)) {
      const items: string[] = []
      while (i < lines.length && /^[-*+]\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^[-*+]\s+/, ''))
        i += 1
      }
      blocks.push({ type: 'unordered-list', items })
      continue
    }

    if (/^\d+\.\s+/.test(line)) {
      const items: string[] = []
      while (i < lines.length && /^\d+\.\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^\d+\.\s+/, ''))
        i += 1
      }
      blocks.push({ type: 'ordered-list', items })
      continue
    }

    if (
      line.includes('|') &&
      i + 1 < lines.length &&
      lines[i + 1].includes('|') &&
      isTableDivider(lines[i + 1])
    ) {
      const headers = splitTableRow(line)
      const aligns = parseTableAlignments(lines[i + 1])
      const rows: string[][] = []
      i += 2
      while (i < lines.length && lines[i].trim() && lines[i].includes('|')) {
        rows.push(splitTableRow(lines[i]))
        i += 1
      }
      blocks.push({ type: 'table', headers, aligns, rows })
      continue
    }

    const paragraph: string[] = []
    while (i < lines.length) {
      const current = lines[i]
      const currentTrimmed = current.trim()
      if (
        !currentTrimmed ||
        current.match(/^```([^\s`]*)\s*$/) ||
        current.match(/^(#{1,6})\s+/) ||
        current.match(/^>\s?/) ||
        current.match(/^[-*+]\s+/) ||
        current.match(/^\d+\.\s+/) ||
        (
          current.includes('|') &&
          i + 1 < lines.length &&
          lines[i + 1].includes('|') &&
          isTableDivider(lines[i + 1])
        )
      ) {
        break
      }
      paragraph.push(current)
      i += 1
    }
    blocks.push({ type: 'paragraph', lines: paragraph })
  }

  return blocks
}

function parseInline(text: string): ReactNode[] {
  const parts: ReactNode[] = []
  let remaining = text
  let key = 0

  while (remaining.length > 0) {
    const match = remaining.match(
      /(`([^`]+)`)|(\[([^\]]+)\]\((https?:\/\/[^)\s]+)\))|(https?:\/\/[^\s<>")\]]+)|(\*\*([^*]+)\*\*)|(__([^_]+)__)|(\*([^*]+)\*)|(_([^_]+)_)/
    )
    if (!match || match.index === undefined) {
      parts.push(remaining)
      break
    }

    if (match.index > 0) {
      parts.push(remaining.slice(0, match.index))
    }

    const token = match[0]
    if (match[1]) {
      parts.push(
        <code key={key++} className="message-inline-code">
          {match[2]}
        </code>,
      )
    } else if (match[3]) {
      parts.push(
        <a
          key={key++}
          href={match[5]}
          target="_blank"
          rel="noreferrer"
          className="message-link"
        >
          {parseInline(match[4])}
        </a>,
      )
    } else if (match[6]) {
      parts.push(
        <a
          key={key++}
          href={match[6]}
          target="_blank"
          rel="noreferrer"
          className="message-link"
        >
          {match[6]}
        </a>,
      )
    } else if (match[7] || match[9]) {
      parts.push(<strong key={key++}>{parseInline(match[8] ?? match[10] ?? '')}</strong>)
    } else if (match[11] || match[13]) {
      parts.push(<em key={key++}>{parseInline(match[12] ?? match[14] ?? '')}</em>)
    }

    remaining = remaining.slice(match.index + token.length)
  }

  return parts
}

function renderParagraphLines(lines: string[]): ReactNode {
  return lines.map((line, index) => (
    <Fragment key={index}>
      {index > 0 && <br />}
      {parseInline(line)}
    </Fragment>
  ))
}

export default function MarkdownMessage({ body }: { body: string }) {
  const blocks = parseBlocks(body)

  return (
    <div className="message-markdown">
      {blocks.map((block, index) => {
        if (block.type === 'heading') {
          if (block.level === 1) {
            return <h1 key={index}>{parseInline(block.text)}</h1>
          }
          if (block.level === 2) {
            return <h2 key={index}>{parseInline(block.text)}</h2>
          }
          if (block.level === 3) {
            return <h3 key={index}>{parseInline(block.text)}</h3>
          }
          if (block.level === 4) {
            return <h4 key={index}>{parseInline(block.text)}</h4>
          }
          if (block.level === 5) {
            return <h5 key={index}>{parseInline(block.text)}</h5>
          }
          return <h6 key={index}>{parseInline(block.text)}</h6>
        }

        if (block.type === 'divider') {
          return <hr key={index} className="message-divider" />
        }

        if (block.type === 'paragraph') {
          return <p key={index}>{renderParagraphLines(block.lines)}</p>
        }

        if (block.type === 'unordered-list') {
          return (
            <ul key={index}>
              {block.items.map((item, itemIndex) => (
                <li key={itemIndex}>{parseInline(item)}</li>
              ))}
            </ul>
          )
        }

        if (block.type === 'ordered-list') {
          return (
            <ol key={index}>
              {block.items.map((item, itemIndex) => (
                <li key={itemIndex}>{parseInline(item)}</li>
              ))}
            </ol>
          )
        }

        if (block.type === 'blockquote') {
          return <blockquote key={index}>{renderParagraphLines(block.lines)}</blockquote>
        }

        if (block.type === 'table') {
          return (
            <div key={index} className="message-table-wrap">
              <table className="message-table">
                <thead>
                  <tr>
                    {block.headers.map((header, cellIndex) => (
                      <th
                        key={cellIndex}
                        style={{ textAlign: block.aligns[cellIndex] ?? 'left' }}
                      >
                        {parseInline(header)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {block.rows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {block.headers.map((_, cellIndex) => (
                        <td
                          key={cellIndex}
                          style={{ textAlign: block.aligns[cellIndex] ?? 'left' }}
                        >
                          {parseInline(row[cellIndex] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        }

        return (
          <pre key={index} className="message-code-block">
            {block.language ? <span className="message-code-language">{block.language}</span> : null}
            <code>{block.content}</code>
          </pre>
        )
      })}
    </div>
  )
}
